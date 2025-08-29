from __future__ import annotations

from collections import defaultdict
from typing import Any

from django.db import transaction

from boranga.components.data_migration.adapters.occurrence import (  # shared canonical schema
    schema,
)
from boranga.components.data_migration.adapters.occurrence.tpfl import (
    OccurrenceTPFLAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.occurrence.models import Occurrence

SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceTPFLAdapter(),
    # Add new adapters here as they are implemented:
    # Source.TEC.value: OccurrenceTECAdapter(),
    # Source.TFAUNA.value: OccurrenceTFAUNAAdapter(),
}


@register
class OccurrenceImporter(BaseSheetImporter):
    slug = "occurrence_legacy"
    description = "Import occurrence data from legacy TEC / TFAUNA / TPFL sources"

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

        # 1. Extract
        for src in sources:
            adapter = SOURCE_ADAPTERS[src]
            src_path = path_map.get(src, path)
            result = adapter.extract(src_path, **options)
            for w in result.warnings:
                warnings.append(f"{src}: {w.message}")
            for r in result.rows:
                r["_source"] = src
            all_rows.extend(result.rows)

        # 2. Build pipelines / same as before
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

        # 3. Transform every row into canonical form, collect per-key groups
        groups: dict[str, list[tuple[dict, str, list[tuple[str, Any]]]]] = defaultdict(
            list
        )
        # groups[legacy_id] -> list of (transformed_dict, source, issues_list)

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
            key = transformed.get("legacy_id")
            if not key:
                # missing key — cannot merge/persist
                skipped += 1
                errors += 1
                continue
            groups[key].append((transformed, row.get("_source"), issues))

        # 4. Merge groups and persist one object per key
        def merge_group(entries, source_priority):
            """
            entries: list of (transformed_dict, source, issues)
            source_priority: list of source keys in preferred order
            Merge rule: for each column pick first non-None/non-empty value
            according to source_priority; combine issues.
            """
            # sort entries by source priority so earlier sources win
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
        for legacy_id, entries in groups.items():
            merged, combined_issues = merge_group(entries, sources)
            # if any error in combined_issues => skip
            if any(i.level == "error" for _, i in combined_issues):
                skipped += 1
                continue

            # build defaults/payload; set legacy_source as joined sources involved
            involved_sources = sorted({src for _, src, _ in entries})
            defaults = {
                "species_code": merged.get("species_code"),
                "observed_date": merged.get("observed_date"),
                "count": merged.get("count"),
                "location_name": merged.get("location_name"),
                "observer_name": merged.get("observer_name"),
                "notes": merged.get("notes"),
                "legacy_source": ",".join(involved_sources),
            }

            if ctx.dry_run:
                # don't persist, but update counters as if created/updated if desired
                continue

            with transaction.atomic():
                obj, created_flag = Occurrence.objects.update_or_create(
                    legacy_id=legacy_id, defaults=defaults
                )
                if created_flag:
                    created += 1
                else:
                    updated += 1

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        return stats
