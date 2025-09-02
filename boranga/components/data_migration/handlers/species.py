from __future__ import annotations

from collections import defaultdict
from typing import Any

from django.db import transaction

from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.adapters.species import schema
from boranga.components.data_migration.adapters.species.tpfl import SpeciesTpflAdapter
from boranga.components.data_migration.mappings import (
    load_legacy_to_pk_map,
    load_species_to_district_links,
    load_species_to_region_links,
)
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.species_and_communities.models import (
    Species,
    SpeciesDistribution,
    SpeciesPublishingStatus,
)

SOURCE_ADAPTERS = {
    Source.TPFL.value: SpeciesTpflAdapter(),
}


@register
class SpeciesImporter(BaseSheetImporter):
    slug = "species_legacy"
    description = "Import species data from legacy source (TPFL)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--sources",
            nargs="+",
            choices=list(SOURCE_ADAPTERS.keys()),
            help="Subset of sources (default: all implemented)",
        )
        parser.add_argument(
            "--path-map",
            nargs="+",
            metavar="SRC=PATH",
            help="Per-source path overrides (e.g. TPFL=/tmp/tpfl.xlsx). If omitted, --path is reused.",
        )

    def _parse_path_map(self, pairs):
        out = {}
        if not pairs:
            return out
        for p in pairs:
            if "=" not in p:
                raise ValueError(f"Invalid path-map entry: {p}")
            k, v = p.split("=", 1)
            out[k] = v
        return out

    def run(self, path: str, ctx: ImportContext, **options):
        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []

        # 1. Extract from each source
        for src in sources:
            adapter = SOURCE_ADAPTERS[src]
            src_path = path_map.get(src, path)
            result = adapter.extract(src_path, **options)
            for w in result.warnings:
                warnings.append(f"{src}: {w.message}")
            for r in result.rows:
                r["_source"] = src
            all_rows.extend(result.rows)

        # 2. Build pipelines from schema
        pipelines = {}
        for col, names in schema.COLUMN_PIPELINES.items():
            from boranga.components.data_migration.registry import (
                registry as transform_registry,
            )

            pipelines[col] = transform_registry.build_pipeline(names)

        processed = 0
        errors = 0
        created = 0
        updated = 0
        skipped = 0
        warn_count = 0

        species_to_region_keys = load_species_to_region_links(legacy_system="TPFL")
        species_to_district_keys = load_species_to_district_links(legacy_system="TPFL")

        # preload region mapping from the other source once (legacy_key -> Region.pk)
        # implement load_legacy_to_pk_map to read LegacyValueMap or build from the other adapter
        region_map = load_legacy_to_pk_map(legacy_system="TPFL", model_name="Region")
        district_map = load_legacy_to_pk_map(
            legacy_system="TPFL", model_name="District"
        )

        # 3. Transform every row into canonical form, collect per-key groups
        groups: dict[str, list[tuple[dict, str, list[tuple[str, Any]]]]] = defaultdict(
            list
        )
        # groups[migrated_from_id] -> list of (transformed_dict, source, issues_list)

        for row in all_rows:
            processed += 1
            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            for col, pipeline in pipelines.items():
                raw_val = row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)
                transformed[col] = res.value
                for issue in res.issues:
                    issues.append((col, issue))
                    if issue.level == "error":
                        has_error = True
                        errors += 1
                    else:
                        warn_count += 1
            if has_error:
                skipped += 1
                continue
            key = transformed.get("migrated_from_id")
            if not key:
                skipped += 1
                errors += 1
                continue
            groups[key].append((transformed, row.get("_source"), issues))

        # 4. Merge groups and persist one object per migrated_from_id
        def merge_group(entries, source_priority):
            entries_sorted = sorted(
                entries,
                key=lambda e: (
                    source_priority.index(e[1])
                    if e[1] in source_priority
                    else len(source_priority)
                ),
            )
            merged = {}
            combined_issues = []
            for col in pipelines.keys():
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(col)
                    if v not in (None, ""):
                        val = v
                        break
                merged[col] = val
            for _, _, iss in entries_sorted:
                combined_issues.extend(iss)
            return merged, combined_issues

        # Persist merged rows
        for migrated_from_id, entries in groups.items():
            merged, combined_issues = merge_group(entries, sources)
            if any(i.level == "error" for _, i in combined_issues):
                skipped += 1
                continue

            # validate cross-field rules using schema helper
            valerrs = schema.validate_species_row(merged)
            if valerrs:
                errors += len(valerrs)
                skipped += 1
                continue

            defaults = {
                "group_type_id": merged.get("group_type_id"),
                "taxonomy_id": merged.get("taxonomy_id"),
                "comment": merged.get("comment"),
                "conservation_plan_exists": merged.get("conservation_plan_exists"),
                "conservation_plan_reference": merged.get(
                    "conservation_plan_reference"
                ),
                "department_file_numbers": merged.get("department_file_numbers"),
                "processing_status": merged.get("processing_status"),
                "submitter": merged.get("submitter"),
                "lodgement_date": merged.get("lodgement_date"),
                "last_data_curation_date": merged.get("last_data_curation_date"),
            }

            if ctx.dry_run:
                continue

            with transaction.atomic():
                obj, created_flag = Species.objects.update_or_create(
                    migrated_from_id=migrated_from_id, defaults=defaults
                )
                if created_flag:
                    created += 1
                else:
                    updated += 1

                # --- create 1-to-1 related ---

                distribution = merged.get("distribution", None)

                SpeciesDistribution.objects.update_or_create(
                    species=obj,
                    defaults={
                        "aoo_actual_auto": False,
                        "distribution": distribution,
                    },
                )

                processing_status_is_active = (
                    merged.get("processing_status") == Species.PROCESSING_STATUS_ACTIVE
                )

                SpeciesPublishingStatus.objects.update_or_create(
                    species=obj,
                    defaults={
                        "conservation_status_public": processing_status_is_active,
                        "distribution_public": processing_status_is_active,
                        "species_public": processing_status_is_active,
                    },
                )

                # --- attach M2M regions ---
                # merged may have e.g. merged['regions'] as a list or delimited string
                if not merged.get("regions"):
                    # prefer explicit regions from species row; fallback to links from separate file
                    merged["regions"] = species_to_region_keys.get(migrated_from_id)

                # now merged["regions"] may be a list or delimited string — same normalization as earlier
                raw_regions = merged.get("regions")
                if raw_regions:
                    # normalize to list of legacy keys
                    if isinstance(raw_regions, str):
                        keys = [k.strip() for k in raw_regions.split(";") if k.strip()]
                    elif isinstance(raw_regions, (list, tuple)):
                        keys = [
                            str(k).strip() for k in raw_regions if k not in (None, "")
                        ]
                    else:
                        keys = [str(raw_regions)]

                    region_ids = []
                    missing = []
                    for legacy_key in keys:
                        pk = region_map.get(legacy_key)
                        if pk:
                            region_ids.append(pk)
                        else:
                            missing.append(legacy_key)

                    if region_ids:
                        # idempotent: replace existing relations with the resolved set
                        obj.regions.set(region_ids)

                    if missing:
                        # record a warning (or TransformIssue earlier); keep lightweight here
                        warn_count += 1
                        warnings.append(
                            f"{migrated_from_id}: unknown region keys {missing}"
                        )

                if not merged.get("districts"):
                    # prefer explicit districts from species row; fallback to links from separate file
                    merged["districts"] = species_to_district_keys.get(migrated_from_id)

                # now merged["districts"] may be a list or delimited string — same normalization as earlier
                raw_districts = merged.get("districts")
                if raw_districts:
                    # normalize to list of legacy keys
                    if isinstance(raw_districts, str):
                        keys = [
                            k.strip() for k in raw_districts.split(";") if k.strip()
                        ]
                    elif isinstance(raw_districts, (list, tuple)):
                        keys = [
                            str(k).strip() for k in raw_districts if k not in (None, "")
                        ]
                    else:
                        keys = [str(raw_districts)]

                    district_ids = []
                    missing = []
                    for legacy_key in keys:
                        pk = district_map.get(legacy_key)
                        if pk:
                            district_ids.append(pk)
                        else:
                            missing.append(legacy_key)

                    if district_ids:
                        # idempotent: replace existing relations with the resolved set
                        obj.districts.set(district_ids)

                    if missing:
                        # record a warning (or TransformIssue earlier); keep lightweight here
                        warn_count += 1
                        warnings.append(
                            f"{migrated_from_id}: unknown district keys {missing}"
                        )

                # --- end M2M attach ---

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        return stats
