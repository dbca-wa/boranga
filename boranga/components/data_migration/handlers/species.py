from __future__ import annotations

import csv
import json
import logging
import os
from collections import defaultdict
from typing import Any

from django.db import transaction
from django.utils import timezone

from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.adapters.species import schema
from boranga.components.data_migration.adapters.species.tpfl import SpeciesTpflAdapter
from boranga.components.data_migration.mappings import (
    load_legacy_to_pk_map,
    load_species_to_district_links,
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

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TPFL.value: SpeciesTpflAdapter(),
}


@register
class SpeciesImporter(BaseSheetImporter):
    slug = "species_legacy"
    description = "Import species data from legacy TEC / TFAUNA / TPFL sources"

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete species target data. Respects `ctx.dry_run` (no-op when True).

        When called with `include_children=True` importer may opt to also clear
        related child tables; this implementation clears obvious related tables
        and works around PROTECT constraints by nullifying FKs where possible
        before deleting parent rows.
        """
        if ctx.dry_run:
            logger.info("SpeciesImporter.clear_targets: dry-run, skipping delete")
            return

        logger.warning("SpeciesImporter: deleting Species and related data...")

        # Perform deletes in an autocommit block so they are committed
        # immediately. This avoids the case where clear_targets runs inside a
        # larger transaction that later rolls back leaving the wipe undone.
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)
        try:
            # Import models lazily to avoid circular imports at module load
            from boranga.components.conservation_status.models import ConservationStatus
            from boranga.components.occurrence.models import (
                Occurrence,
                OccurrenceReport,
                OccurrenceReportApprovalDetails,
            )

            # 1) Handle Occurrences -> must remove/clear PROTECT references first
            occ_qs = Occurrence.objects.all()
            if occ_qs.exists():
                occ_ids = list(occ_qs.values_list("id", flat=True))

                # Nullify approval details that point to these occurrences
                if OccurrenceReportApprovalDetails is not None:
                    try:
                        (
                            OccurrenceReportApprovalDetails.objects.filter(
                                occurrence_id__in=occ_ids
                            ).update(occurrence=None)
                        )
                    except Exception:
                        logger.exception(
                            "Failed to nullify OccurrenceReportApprovalDetails.occurrence"
                        )
                        raise

                # Nullify occurrence FK on OccurrenceReport
                try:
                    OccurrenceReport.objects.filter(occurrence_id__in=occ_ids).update(
                        occurrence=None
                    )
                except Exception:
                    logger.exception("Failed to nullify OccurrenceReport.occurrence")
                    raise

                # Clear combined_occurrence references pointing to these occurrences
                try:
                    Occurrence.objects.filter(
                        combined_occurrence_id__in=occ_ids
                    ).update(combined_occurrence=None)
                except Exception:
                    logger.exception(
                        "Failed to clear Occurrence.combined_occurrence references"
                    )
                    raise

                # Now safe to delete the occurrence rows
                try:
                    occ_qs.delete()
                except Exception:
                    logger.exception("Failed to delete Occurrence objects")
                    raise

            # 2) Delete any remaining OccurrenceReport objects that reference Species
            try:
                OccurrenceReport.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OccurrenceReport objects")
                raise

            # 3) Delete ConservationStatus that reference Species
            try:
                ConservationStatus.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete ConservationStatus")
                raise

            # 4) Delete species-related helper tables and species
            try:
                SpeciesPublishingStatus.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete SpeciesPublishingStatus")
                raise
            try:
                SpeciesDistribution.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete SpeciesDistribution")
                raise
            try:
                Species.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete Species")
                raise
        finally:
            if not was_autocommit:
                conn.set_autocommit(False)

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
        start_time = timezone.now()
        logger.info(
            "SpeciesImporter (%s) started at %s (dry_run=%s)",
            self.slug,
            start_time.isoformat(),
            ctx.dry_run,
        )

        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []
        errors_details = []  # collect detailed error records
        warnings_details = []  # collect detailed warning records

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

        species_to_district_keys = load_species_to_district_links(legacy_system="TPFL")

        # preload region mapping from the other source once (legacy_key -> Region.pk)
        # implement load_legacy_to_pk_map to read LegacyValueMap or build from the other adapter
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
            # progress output every 500 rows
            if processed % 500 == 0:
                logger.info(
                    "SpeciesImporter %s: processed %d rows so far", self.slug, processed
                )

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
                    # collect detailed issue info for later reporting
                    record = {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": col,
                        "level": getattr(issue, "level", "error"),
                        "message": getattr(issue, "message", str(issue)),
                        "raw_value": raw_val,
                    }
                    if getattr(issue, "level", "error") == "error":
                        has_error = True
                        errors += 1
                        errors_details.append(record)
                    else:
                        warn_count += 1
                        warnings_details.append(record)
            if has_error:
                skipped += 1
                continue

            # copy adapter-added keys (e.g. group_type_id) from the source row into
            # the transformed dict so they survive the merge. Skip internals.
            for k, v in row.items():
                if k.startswith("_"):
                    continue
                if k in transformed:
                    continue
                transformed[k] = v

            key = transformed.get("migrated_from_id")
            if not key:
                skipped += 1
                errors += 1
                errors_details.append(
                    {"reason": "missing_migrated_from_id", "row": transformed}
                )
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
            # first merge the canonical columns defined by the schema/pipelines
            for col in pipelines.keys():
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(col)
                    if v not in (None, ""):
                        val = v
                        break
                merged[col] = val
            # also merge any adapter-added keys (e.g. group_type_id) that are present
            # in the transformed dicts but not in the schema pipelines
            extra_keys = set().union(
                *(set(trans.keys()) for trans, _, _ in entries_sorted)
            )
            for extra in sorted(extra_keys):
                if extra in pipelines:
                    continue
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(extra)
                    if v not in (None, ""):
                        val = v
                        break
                merged[extra] = val
            for _, _, iss in entries_sorted:
                combined_issues.extend(iss)
            return merged, combined_issues

        # Persist merged rows in two phases to reduce per-row DB operations:
        # 1) Collect all entries and prepare create/update operations
        # 2) Bulk create new Species and bulk create related one-to-one rows where possible
        ops = []
        persisted = 0
        for migrated_from_id, entries in groups.items():
            persisted += 1
            if persisted % 500 == 0:
                logger.info(
                    "SpeciesImporter %s: prepared %d groups so far",
                    self.slug,
                    persisted,
                )

            merged, combined_issues = merge_group(entries, sources)
            if any(i.level == "error" for _, i in combined_issues):
                skipped += 1
                continue

            # validate cross-field rules using schema helper
            valerrs = schema.validate_species_row(merged)
            if valerrs:
                errors += len(valerrs)
                skipped += 1
                for ve in valerrs:
                    errors_details.append(
                        {
                            "migrated_from_id": merged.get("migrated_from_id"),
                            "reason": "validation_error",
                            "message": str(ve),
                            "row": merged,
                        }
                    )
                continue

            defaults = {
                "migrated_from_id": merged.get("migrated_from_id"),
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
                logger.debug(
                    "SpeciesImporter %s dry-run: would persist migrated_from_id=%s ",
                    self.slug,
                    migrated_from_id,
                )
                continue

            # capture relevant M2M district data for later processing
            districts_raw = merged.get("districts") or species_to_district_keys.get(
                migrated_from_id
            )

            ops.append(
                {
                    "migrated_from_id": migrated_from_id,
                    "defaults": defaults,
                    "merged": merged,
                    "districts_raw": districts_raw,
                }
            )

        # Prefetch existing Species by migrated_from_id and taxonomy_id to avoid N queries
        migrated_keys = [o["migrated_from_id"] for o in ops]
        taxonomy_ids = [
            o["defaults"].get("taxonomy_id")
            for o in ops
            if o["defaults"].get("taxonomy_id")
        ]
        existing_by_migrated = {
            s.migrated_from_id: s
            for s in Species.objects.filter(migrated_from_id__in=migrated_keys)
        }
        existing_by_taxonomy = {}
        if taxonomy_ids:
            existing_by_taxonomy = {
                s.taxonomy_id: s
                for s in Species.objects.filter(taxonomy_id__in=taxonomy_ids)
            }

        # Prepare lists for bulk operations
        to_create = []  # Species instances to bulk_create
        create_meta = []  # parallel metadata to handle related rows after creation
        to_update = []  # existing Species instances to bulk_update
        # Handle duplicate taxonomy_id values across ops to avoid unique constraint
        tax_seen = {}  # taxonomy_id -> canonical migrated_from_id
        pending_updates = (
            {}
        )  # canonical_migrated -> list of (migrated_from_id, defaults, merged, districts_raw)

        for op in ops:
            migrated_from_id = op["migrated_from_id"]
            defaults = op["defaults"]
            merged = op["merged"]
            districts_raw = op.get("districts_raw")

            obj = existing_by_migrated.get(migrated_from_id)
            if obj:
                # update existing
                for k, v in defaults.items():
                    setattr(obj, k, v)
                to_update.append((obj, merged, districts_raw))
                continue

            taxonomy_id = defaults.get("taxonomy_id")
            existing = None
            if taxonomy_id:
                existing = existing_by_taxonomy.get(taxonomy_id)

            if existing:
                # update existing species rather than creating new
                for k, v in defaults.items():
                    setattr(existing, k, v)
                existing.migrated_from_id = migrated_from_id
                to_update.append((existing, merged, districts_raw))
            elif taxonomy_id and taxonomy_id in tax_seen:
                # Another op in this run has already claimed this taxonomy_id.
                # Defer applying this op as an update to the canonical record after it's created.
                canonical = tax_seen[taxonomy_id]
                logger.info(
                    "SpeciesImporter: taxonomy collision detected for taxonomy_id=%s; "
                    "deferring migrated_from_id=%s to canonical migrated_from_id=%s",
                    taxonomy_id,
                    migrated_from_id,
                    canonical,
                )
                pending_updates.setdefault(canonical, []).append(
                    (migrated_from_id, defaults, merged, districts_raw)
                )
            elif taxonomy_id:
                # First time we see this taxonomy_id and it doesn't exist in DB.
                # Claim it as canonical and create one instance for it.
                tax_seen[taxonomy_id] = migrated_from_id
                logger.debug(
                    "SpeciesImporter: claiming taxonomy_id=%s for migrated_from_id=%s",
                    taxonomy_id,
                    migrated_from_id,
                )
                create_kwargs = dict(defaults)
                create_kwargs["migrated_from_id"] = migrated_from_id
                if getattr(ctx, "migration_run", None) is not None:
                    create_kwargs["migration_run"] = ctx.migration_run
                inst = Species(**create_kwargs)
                to_create.append(inst)
                create_meta.append((migrated_from_id, merged, districts_raw))
            else:
                # No taxonomy_id: safe to create a separate Species
                create_kwargs = dict(defaults)
                create_kwargs["migrated_from_id"] = migrated_from_id
                if getattr(ctx, "migration_run", None) is not None:
                    create_kwargs["migration_run"] = ctx.migration_run
                inst = Species(**create_kwargs)
                to_create.append(inst)
                create_meta.append((migrated_from_id, merged, districts_raw))

        # Bulk create new Species in chunks
        BATCH = 1000
        created_map = {}  # migrated_from_id -> Species instance (with pk)
        if to_create:
            logger.info("SpeciesImporter: bulk-creating %d new Species", len(to_create))
            for i in range(0, len(to_create), BATCH):
                chunk = to_create[i : i + BATCH]
                with transaction.atomic():
                    Species.objects.bulk_create(chunk, batch_size=BATCH)

        # Refresh created objects: query DB for all migrated_from_ids we just created
        if create_meta:
            created_keys = [m[0] for m in create_meta]
            for s in Species.objects.filter(migrated_from_id__in=created_keys):
                created_map[s.migrated_from_id] = s

        # Apply any pending updates that were deferred because they shared a taxonomy_id
        if "pending_updates" in locals() and pending_updates:
            # build reverse map so we can log taxonomy_id for a canonical migrated key
            canonical_to_tax = (
                {v: k for k, v in tax_seen.items()} if "tax_seen" in locals() else {}
            )
            for canonical_mig, pendings in pending_updates.items():
                taxonomy_id_for_canonical = canonical_to_tax.get(canonical_mig)
                # canonical may be in created_map (newly created) or existing_by_migrated
                obj = created_map.get(canonical_mig) or existing_by_migrated.get(
                    canonical_mig
                )
                if not obj:
                    logger.warning(
                        "SpeciesImporter: canonical object for taxonomy pending_updates not found: %s",
                        canonical_mig,
                    )
                    continue
                # Log what we're about to merge for auditing
                pending_ids = [p[0] for p in pendings]
                logger.info(
                    "SpeciesImporter: applying %d pending updates to canonical migrated_from_id=%s "
                    "(taxonomy_id=%s): %s",
                    len(pendings),
                    canonical_mig,
                    taxonomy_id_for_canonical,
                    pending_ids,
                )
                # Apply each pending update in order; later pendings overwrite earlier
                for pending in pendings:
                    pending_mig, pending_defaults, pending_merged, pending_districts = (
                        pending
                    )
                    for k, v in pending_defaults.items():
                        setattr(obj, k, v)
                    # set migrated_from_id to the pending migrated id to match original behavior
                    obj.migrated_from_id = pending_mig
                    to_update.append((obj, pending_merged, pending_districts))

        # Bulk update existing objects collected in to_update
        if to_update:
            logger.info(
                "SpeciesImporter: bulk-updating %d existing Species", len(to_update)
            )
            # extract instances and update fields en-masse
            update_instances = [t[0] for t in to_update]
            # determine fields to update from defaults keys
            if update_instances:
                fields = list(op["defaults"].keys())
                # ensure migrated_from_id included if we set it
                if "migrated_from_id" not in fields:
                    fields.append("migrated_from_id")
                Species.objects.bulk_update(update_instances, fields, batch_size=BATCH)

        # Now handle related one-to-one models and M2Ms for created and updated species
        # We'll batch SpeciesDistribution and SpeciesPublishingStatus updates/creates
        # and perform M2M assignments via through models to avoid per-object queries.
        dist_to_create = []
        dist_to_update = []
        publish_to_create = []
        publish_to_update = []

        # Collect desired M2M pairs (species_id, district_id) and (species_id, region_id)
        desired_district_pairs = set()
        desired_region_pairs = set()

        # helper to normalize district raw values into a list of district PKs
        from boranga.components.species_and_communities.models import District

        def parse_district_keys(raw):
            if not raw:
                return [], []
            if isinstance(raw, str):
                keys = [k.strip() for k in raw.split(";") if k.strip()]
            elif isinstance(raw, (list, tuple)):
                keys = [str(k).strip() for k in raw if k not in (None, "")]
            else:
                keys = [str(raw)]
            district_ids = []
            missing = []
            for legacy_key in keys:
                pk = district_map.get(legacy_key)
                if pk:
                    district_ids.append(pk)
                else:
                    missing.append(legacy_key)
            return district_ids, missing

        # Process updates: collect distributions/publishing updates and desired M2M pairs
        updated_species_ids = []
        for existing_tuple in to_update:
            obj, merged, districts_raw = existing_tuple
            updated += 1
            updated_species_ids.append(obj.pk)

            # prepare distribution update/create
            distribution = merged.get("distribution", None)
            dist_to_update.append((obj.pk, distribution))

            processing_status_is_active = (
                merged.get("processing_status") == Species.PROCESSING_STATUS_ACTIVE
            )
            publish_to_update.append((obj.pk, processing_status_is_active))

            # collect desired district/region pairs for this species
            district_ids, missing = parse_district_keys(districts_raw)
            if missing:
                warn_count += 1
                warnings.append(
                    f"{obj.migrated_from_id}: unknown district keys {missing}"
                )
                warnings_details.append(
                    {
                        "migrated_from_id": obj.migrated_from_id,
                        "missing_districts": missing,
                    }
                )
            for did in district_ids:
                desired_district_pairs.add((obj.pk, did))

        # Process created instances similarly
        created_species_ids = []
        if create_meta:
            for migrated_from_id, merged, districts_raw in create_meta:
                obj = created_map.get(migrated_from_id)
                if not obj:
                    logger.warning(
                        "Created Species not found for migrated_from_id=%s",
                        migrated_from_id,
                    )
                    continue
                created += 1
                created_species_ids.append(obj.pk)
                distribution = merged.get("distribution", None)
                dist_to_create.append(
                    SpeciesDistribution(
                        species=obj, aoo_actual_auto=False, distribution=distribution
                    )
                )

                processing_status_is_active = (
                    merged.get("processing_status") == Species.PROCESSING_STATUS_ACTIVE
                )
                publish_to_create.append(
                    SpeciesPublishingStatus(
                        species=obj,
                        conservation_status_public=processing_status_is_active,
                        distribution_public=processing_status_is_active,
                        species_public=processing_status_is_active,
                        threats_public=processing_status_is_active,
                    )
                )

                district_ids, missing = parse_district_keys(districts_raw)
                if missing:
                    warn_count += 1
                    warnings.append(
                        f"{migrated_from_id}: unknown district keys {missing}"
                    )
                    warnings_details.append(
                        {
                            "migrated_from_id": migrated_from_id,
                            "missing_districts": missing,
                        }
                    )
                for did in district_ids:
                    desired_district_pairs.add((obj.pk, did))

        # Bulk create distributions and publishing statuses for created species
        if dist_to_create:
            try:
                SpeciesDistribution.objects.bulk_create(
                    dist_to_create, batch_size=BATCH
                )
            except Exception as e:
                # Record the failure so it's surfaced in stats/errors_details
                errors += 1
                errors_details.append(
                    {
                        "reason": "bulk_create_speciesdistribution_failed",
                        "message": str(e),
                        "species_ids": [
                            getattr(d.species, "pk", None) for d in dist_to_create
                        ],
                    }
                )
                logger.exception(
                    "Failed to bulk_create SpeciesDistribution; falling back to individual creates"
                )
                for d in dist_to_create:
                    try:
                        SpeciesDistribution.objects.update_or_create(
                            species=d.species,
                            defaults={
                                "aoo_actual_auto": d.aoo_actual_auto,
                                "distribution": d.distribution,
                            },
                        )
                    except Exception:
                        logger.exception(
                            "Failed to create SpeciesDistribution for %s", d.species.pk
                        )

        if publish_to_create:
            try:
                SpeciesPublishingStatus.objects.bulk_create(
                    publish_to_create, batch_size=BATCH
                )
            except Exception as e:
                errors += 1
                errors_details.append(
                    {
                        "reason": "bulk_create_speciespublishing_failed",
                        "message": str(e),
                        "species_ids": [
                            getattr(p.species, "pk", None) for p in publish_to_create
                        ],
                    }
                )
                logger.exception(
                    "Failed to bulk_create SpeciesPublishingStatus; falling back to individual creates"
                )
                for p in publish_to_create:
                    try:
                        SpeciesPublishingStatus.objects.update_or_create(
                            species=p.species,
                            defaults={
                                "conservation_status_public": p.conservation_status_public,
                                "distribution_public": p.distribution_public,
                                "species_public": p.species_public,
                                "threats_public": p.threats_public,
                            },
                        )
                    except Exception:
                        logger.exception(
                            "Failed to create SpeciesPublishingStatus for %s",
                            p.species.pk,
                        )

        # Handle updates for SpeciesDistribution and SpeciesPublishingStatus in bulk
        # Distributions: fetch existing for updated species and update or create as needed
        if dist_to_update:
            species_ids = [sid for sid, _ in dist_to_update]
            existing_dists = {
                d.species_id: d
                for d in SpeciesDistribution.objects.filter(species_id__in=species_ids)
            }
            to_create_dists = []
            to_update_dists = []
            for sid, distribution in dist_to_update:
                if sid in existing_dists:
                    inst = existing_dists[sid]
                    inst.distribution = distribution
                    inst.aoo_actual_auto = False
                    to_update_dists.append(inst)
                else:
                    # species instance available in DB
                    to_create_dists.append(
                        SpeciesDistribution(
                            species_id=sid,
                            aoo_actual_auto=False,
                            distribution=distribution,
                        )
                    )

            if to_update_dists:
                try:
                    SpeciesDistribution.objects.bulk_update(
                        to_update_dists,
                        ["distribution", "aoo_actual_auto"],
                        batch_size=BATCH,
                    )
                except Exception:
                    logger.exception(
                        "Failed to bulk_update SpeciesDistribution; will fallback to individual saves"
                    )
                    for inst in to_update_dists:
                        try:
                            inst.save()
                        except Exception:
                            logger.exception(
                                "Failed to save SpeciesDistribution for %s",
                                inst.species_id,
                            )

            if to_create_dists:
                try:
                    SpeciesDistribution.objects.bulk_create(
                        to_create_dists, batch_size=BATCH
                    )
                except Exception:
                    logger.exception(
                        "Failed to bulk_create SpeciesDistribution (creates); falling back to individual creates"
                    )
                    for inst in to_create_dists:
                        try:
                            inst.save()
                        except Exception:
                            logger.exception(
                                "Failed to create SpeciesDistribution for %s",
                                inst.species_id,
                            )

        # Publishing statuses: similar approach
        if publish_to_update:
            species_ids = [sid for sid, _ in publish_to_update]
            existing_pubs = {
                p.species_id: p
                for p in SpeciesPublishingStatus.objects.filter(
                    species_id__in=species_ids
                )
            }
            to_create_pubs = []
            to_update_pubs = []
            for sid, active in publish_to_update:
                if sid in existing_pubs:
                    inst = existing_pubs[sid]
                    inst.conservation_status_public = active
                    inst.distribution_public = active
                    inst.species_public = active
                    inst.threats_public = active
                    to_update_pubs.append(inst)
                else:
                    to_create_pubs.append(
                        SpeciesPublishingStatus(
                            species_id=sid,
                            conservation_status_public=active,
                            distribution_public=active,
                            species_public=active,
                            threats_public=active,
                        )
                    )

            if to_update_pubs:
                try:
                    SpeciesPublishingStatus.objects.bulk_update(
                        to_update_pubs,
                        [
                            "conservation_status_public",
                            "distribution_public",
                            "species_public",
                            "threats_public",
                        ],
                        batch_size=BATCH,
                    )
                except Exception:
                    logger.exception(
                        "Failed to bulk_update SpeciesPublishingStatus; will fallback to individual saves"
                    )
                    for inst in to_update_pubs:
                        try:
                            inst.save()
                        except Exception:
                            logger.exception(
                                "Failed to save SpeciesPublishingStatus for %s",
                                inst.species_id,
                            )

            if to_create_pubs:
                try:
                    SpeciesPublishingStatus.objects.bulk_create(
                        to_create_pubs, batch_size=BATCH
                    )
                except Exception:
                    logger.exception(
                        "Failed to bulk_create SpeciesPublishingStatus (creates); falling back to individual creates"
                    )
                    for inst in to_create_pubs:
                        try:
                            inst.save()
                        except Exception:
                            logger.exception(
                                "Failed to create SpeciesPublishingStatus for %s",
                                inst.species_id,
                            )

        # Now handle M2M assignments using through models for performance
        if desired_district_pairs:
            # collect all species ids and district ids involved
            all_species_ids = {sid for sid, _ in desired_district_pairs}
            all_district_ids = {did for _, did in desired_district_pairs}

            # map district -> region
            district_region_map = dict(
                District.objects.filter(id__in=all_district_ids).values_list(
                    "id", "region_id"
                )
            )

            # build desired region pairs from desired_district_pairs
            for sid, did in list(desired_district_pairs):
                region_id = district_region_map.get(did)
                if region_id:
                    desired_region_pairs.add((sid, region_id))

            # through models
            DistrictThrough = Species.districts.through
            RegionThrough = Species.regions.through

            # Delete existing m2m links for these species and bulk create desired ones
            try:
                with transaction.atomic():
                    DistrictThrough.objects.filter(
                        species_id__in=all_species_ids
                    ).delete()
                    RegionThrough.objects.filter(
                        species_id__in=all_species_ids
                    ).delete()

                    # bulk create district links
                    district_objs = [
                        DistrictThrough(species_id=sid, district_id=did)
                        for sid, did in desired_district_pairs
                    ]
                    if district_objs:
                        DistrictThrough.objects.bulk_create(
                            district_objs, batch_size=BATCH
                        )

                    # bulk create region links
                    region_objs = [
                        RegionThrough(species_id=sid, region_id=rid)
                        for sid, rid in desired_region_pairs
                    ]
                    if region_objs:
                        RegionThrough.objects.bulk_create(region_objs, batch_size=BATCH)
            except Exception:
                logger.exception(
                    "Failed to bulk update M2M district/region links; falling back to per-object sets"
                )
                # fallback to previous safe behavior
                for sid in all_species_ids:
                    try:
                        species_obj = Species.objects.get(pk=sid)
                        # compute district ids for this species from desired_district_pairs
                        dids = [did for s, did in desired_district_pairs if s == sid]
                        if dids:
                            species_obj.districts.set(dids)
                        # compute region ids
                        rids = {
                            district_region_map.get(did)
                            for did in dids
                            if district_region_map.get(did)
                        }
                        if rids:
                            species_obj.regions.set(list(rids))
                    except Exception:
                        logger.exception("Fallback M2M set failed for species %s", sid)

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        # do NOT attach the full error/warning lists (caller code often prints stats -> huge dumps)
        # keep lightweight: counts, messages and CSV path (set below if written)
        stats["error_count_details"] = len(errors_details)
        stats["warning_count_details"] = len(warnings_details)
        stats["warning_messages"] = warnings
        stats["error_details_csv"] = None

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)

        # write detailed errors to CSV (if any) but only log a concise count summary
        if errors_details:
            # allow override via options, otherwise write to
            # <cwd>/boranga/components/data_migration/handlers/handler_output with timestamp
            csv_path = options.get("error_csv")
            if csv_path:
                csv_path = os.path.abspath(csv_path)
            else:
                ts = timezone.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(
                    os.getcwd(),
                    "boranga/components/data_migration/handlers/handler_output",
                    f"{self.slug}_errors_{ts}.csv",
                )

            logger.info("Writing SpeciesImporter error CSV to %s", csv_path)

            try:
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                with open(csv_path, "w", newline="", encoding="utf-8") as fh:
                    fieldnames = [
                        "migrated_from_id",
                        "column",
                        "level",
                        "message",
                        "raw_value",
                        "reason",
                        "row_json",
                        "timestamp",
                    ]
                    writer = csv.DictWriter(fh, fieldnames=fieldnames)
                    writer.writeheader()
                    for rec in errors_details:
                        writer.writerow(
                            {
                                "migrated_from_id": rec.get("migrated_from_id"),
                                "column": rec.get("column"),
                                "level": rec.get("level"),
                                "message": rec.get("message"),
                                "raw_value": rec.get("raw_value"),
                                "reason": rec.get("reason"),
                                "row_json": json.dumps(rec.get("row", ""), default=str),
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                # record CSV location on stats (small and safe to print)
                stats["error_details_csv"] = csv_path
                logger.info(
                    (
                        "SpeciesImporter %s finished; processed=%d created=%d "
                        "updated=%d skipped=%d errors=%d warnings=%d time_taken=%s (details -> %s)",
                    ),
                    self.slug,
                    processed,
                    created,
                    updated,
                    skipped,
                    errors,
                    warn_count,
                    str(elapsed),
                    csv_path,
                )
            except Exception as e:
                logger.error(
                    "Failed to write error CSV for %s at %s: %s",
                    self.slug,
                    csv_path,
                    e,
                )
                # still log concise summary
                logger.info(
                    (
                        "SpeciesImporter %s finished; processed=%d created=%d "
                        "updated=%d skipped=%d errors=%d warnings=%d time_taken=%s"
                    ),
                    self.slug,
                    processed,
                    created,
                    updated,
                    skipped,
                    errors,
                    warn_count,
                    str(elapsed),
                )
        else:
            # no detailed errors: concise summary only
            logger.info(
                (
                    "SpeciesImporter %s finished; processed=%d created=%d updated=%d"
                    " skipped=%d errors=%d warnings=%d time_taken=%s",
                ),
                self.slug,
                processed,
                created,
                updated,
                skipped,
                errors,
                warn_count,
                str(elapsed),
            )

        return stats
