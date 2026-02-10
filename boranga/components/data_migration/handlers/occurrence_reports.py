from __future__ import annotations

import difflib
import json
import logging
import os
from collections import defaultdict
from typing import Any

from django.core.exceptions import FieldDoesNotExist
from django.db import models as dj_models
from django.db import transaction
from django.utils import timezone

from boranga.components.data_migration.adapters.occurrence_report import schema
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.handlers.helpers import (
    apply_value_to_instance,
    normalize_create_kwargs,
)
from boranga.components.data_migration.mappings import (
    load_sheet_associated_species_names,
)
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.main.models import LegacyTaxonomyMapping
from boranga.components.occurrence.models import (
    AssociatedSpeciesTaxonomy,
    Intensity,
    OCCAssociatedSpecies,
    OCCFireHistory,
    OCCHabitatComposition,
    OCCHabitatCondition,
    OCCIdentification,
    OCCLocation,
    OCCObservationDetail,
    OCCPlantCount,
    Occurrence,
    OccurrenceGeometry,
    OccurrenceReport,
    OccurrenceReportDocument,
    OccurrenceReportGeometry,
    OccurrenceReportUserAction,
    OCCVegetationStructure,
    OCRAnimalObservation,
    OCRAssociatedSpecies,
    OCRFireHistory,
    OCRHabitatComposition,
    OCRHabitatCondition,
    OCRIdentification,
    OCRLocation,
    OCRObservationDetail,
    OCRObserverDetail,
    OCRPlantCount,
    OCRVegetationStructure,
)
from boranga.components.species_and_communities.models import (
    DocumentCategory,
    DocumentSubCategory,
    Taxonomy,
)
from boranga.components.users.models import SubmitterInformation

logger = logging.getLogger(__name__)

# Map adapter keys to adapter classes (not instances) so we can lazily
# instantiate adapters after import-time. Some adapters perform expensive
# setup in their constructor which can block the management command
# startup and hide early logs. We instantiate them lazily in `run()`
# just before calling `extract()` and cache the instance back into this
# dict for subsequent use.
SOURCE_ADAPTERS = {
    # Use dotted path so the adapter module isn't imported at module import
    # time. We'll import the class lazily inside `run()` after emitting
    # initial logs to avoid long silent startup delays.
    Source.TPFL.value: "boranga.components.data_migration.adapters.occurrence_report.tpfl.OccurrenceReportTpflAdapter",
    Source.TEC_SITE_VISITS.value: (
        "boranga.components.data_migration.adapters.occurrence_report.tec_site_visits.OccurrenceReportTecSiteVisitsAdapter"
    ),
    Source.TEC_SURVEYS.value: (
        "boranga.components.data_migration.adapters.occurrence_report.tec_surveys.OccurrenceReportTecSurveysAdapter"
    ),
    Source.TFAUNA.value: (
        "boranga.components.data_migration.adapters.occurrence_report.tfauna.OccurrenceReportTfaunaAdapter"
    ),
    # add other adapters when available
}


@register
class OccurrenceReportImporter(BaseSheetImporter):
    slug = "occurrence_report_legacy"
    description = "Import occurrence reports from legacy sources (TPFL etc)"

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """Delete OccurrenceReport target data and its child tables. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            logger.info("OccurrenceReportImporter.clear_targets: dry-run, skipping delete")
            return

        from boranga.components.data_migration.adapters.sources import (
            SOURCE_GROUP_TYPE_MAP,
        )

        sources = options.get("sources")
        target_group_types = set()
        if sources:
            for s in sources:
                if s in SOURCE_GROUP_TYPE_MAP:
                    target_group_types.add(SOURCE_GROUP_TYPE_MAP[s])

        is_filtered = bool(sources)

        if is_filtered:
            if not target_group_types:
                logger.warning(
                    "clear_targets: sources %s provided but no associated group_types found in map. Skipping delete.",
                    sources,
                )
                return
            logger.warning(
                "OccurrenceReportImporter: deleting OccurrenceReport and related data for group_types: %s ...",
                target_group_types,
            )
            report_filter = {"group_type__name__in": target_group_types}
        else:
            logger.warning("OccurrenceReportImporter: deleting OccurrenceReport and related data...")
            report_filter = {}

        # Perform deletes in an autocommit block so they are committed
        # immediately. This mirrors the approach used in `SpeciesImporter` and
        # allows us to reset DB sequences safely after the delete.
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)
        try:
            try:
                if is_filtered:
                    OccurrenceReport.objects.filter(**report_filter).delete()
                else:
                    OccurrenceReport.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OccurrenceReport")

            # Reset the primary key sequence for OccurrenceReport when using PostgreSQL.
            try:
                if getattr(conn, "vendor", None) == "postgresql":
                    table = OccurrenceReport._meta.db_table
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT MAX(id) FROM {table}")
                        row = cur.fetchone()
                        max_id = row[0] if row else None

                        if max_id is not None:
                            cur.execute(
                                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                                [table, "id", max_id, True],
                            )
                        else:
                            cur.execute(
                                "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                                [table, "id", 1, False],
                            )
                    logger.info("Reset primary key sequence for table %s to %s", table, max_id)
            except Exception:
                logger.exception("Failed to reset OccurrenceReport primary key sequence")
        finally:
            if not was_autocommit:
                conn.set_autocommit(False)

    def preload_pop_section_map(self, path: str) -> dict[str, list[tuple[str, str]]]:
        """
        Load DRF_POP_SECTION_MAP.csv into a dict:
        SHEETNO -> [(POP_ID, SECT_CODE), ...]
        """
        import csv
        import os

        # Try to find the file in the same directory as the input path first.
        base_dir = os.path.dirname(path)
        map_path = os.path.join(base_dir, "DRF_POP_SECTION_MAP.csv")

        if not os.path.exists(map_path):
            # Fallback to the known location if the input path is different
            map_path = "private-media/legacy_data/TPFL/DRF_POP_SECTION_MAP.csv"

        if not os.path.exists(map_path):
            logger.warning(f"DRF_POP_SECTION_MAP.csv not found at {map_path}. Skipping section copying.")
            return {}

        mapping = defaultdict(list)
        try:
            with open(map_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sheetno = row.get("SHEETNO", "").strip()
                    pop_id = row.get("POP_ID", "").strip()
                    sect_code = row.get("SECT_CODE", "").strip()
                    is_active = row.get("IS_ACTIVE", "").strip()

                    if sheetno and pop_id and sect_code and is_active == "Y":
                        mapping[sheetno].append((pop_id, sect_code))

            logger.info(f"Loaded {len(mapping)} entries from {map_path}")
            return mapping
        except Exception as e:
            logger.exception(f"Failed to load DRF_POP_SECTION_MAP.csv: {e}")
            return {}

    def preload_sheet_vws_map(self, path: str) -> dict[str, str]:
        """
        Load DRF_SHEET_VWS.csv into a dict:
        SHEETNO -> POP_ID
        """
        import csv
        import os

        # Try to find the file in the same directory as the input path first.
        base_dir = os.path.dirname(path)
        map_path = os.path.join(base_dir, "DRF_SHEET_VWS.csv")

        if not os.path.exists(map_path):
            # Fallback to the known location if the input path is different
            map_path = "private-media/legacy_data/TPFL/DRF_SHEET_VWS.csv"

        if not os.path.exists(map_path):
            logger.warning(f"DRF_SHEET_VWS.csv not found at {map_path}. Skipping fallback linking.")
            return {}

        mapping = {}
        try:
            with open(map_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sheetno = row.get("SHEETNO", "").strip()
                    pop_id = row.get("POP_ID", "").strip()
                    if sheetno and pop_id:
                        mapping[sheetno.casefold()] = pop_id

            logger.info(f"Loaded {len(mapping)} entries from {map_path}")
            return mapping
        except Exception as e:
            logger.exception(f"Failed to load DRF_SHEET_VWS.csv: {e}")
            return {}

    def preload_tec_site_species_map(self, path: str) -> dict[str, list[dict]]:
        """
        Load SITE_SPECIES.csv into a dict:
        SITE_VISIT_ID -> [{taxon_name_id, comments, ...}, ...]
        """
        import csv
        import os
        from collections import defaultdict

        base_dir = os.path.dirname(path)
        map_path = os.path.join(base_dir, "SITE_SPECIES.csv")

        # If input path is SITE_VISITS.csv, map_path should be correct.
        if not os.path.exists(map_path) and "SITE_VISITS" in path:
            # Try replacing SITE_VISITS with SITE_SPECIES if naming convention differs
            map_path = path.replace("SITE_VISITS", "SITE_SPECIES").replace(".csv", ".csv")

        if not os.path.exists(map_path):
            # Fallback for dev/test environments
            map_path = "private-media/legacy_data/TEC/SITE_SPECIES.csv"

        if not os.path.exists(map_path):
            logger.warning(
                "SITE_SPECIES.csv not found at %s. Skipping TEC associated species loading.",
                map_path,
            )
            return {}

        mapping = defaultdict(list)
        try:
            with open(map_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    visit_id = row.get("SITE_VISIT_ID", "").strip()
                    if not visit_id:
                        continue

                    # Extract fields
                    taxon_id = row.get("taxon_name_id", "").strip()
                    # Comments construction: SSP_NOTES + SSP_HEIGHT + SSP_COLLECTOR_CODE + SSP_COLLECTION_NUMBER
                    notes = row.get("SSP_NOTES", "").strip()
                    height = row.get("SSP_HEIGHT", "").strip()
                    coll_code = row.get("SSP_COLLECTOR_CODE", "").strip()
                    coll_num = row.get("SSP_COLLECTION_NUMBER", "").strip()

                    parts = []
                    if notes:
                        parts.append(notes)
                    if height:
                        parts.append(f"Height: {height}")
                    if coll_code:
                        parts.append(f"Collector Code: {coll_code}")
                    if coll_num:
                        parts.append(f"Collector Number: {coll_num}")

                    comments = "; ".join(parts)

                    mapping[visit_id].append({"taxon_name_id": taxon_id, "comments": comments})

            logger.info(f"Loaded associated species for {len(mapping)} visits from {map_path}")
            return mapping
        except Exception as e:
            logger.warning(f"Failed to load SITE_SPECIES.csv: {e}")
            return {}

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
        parser.add_argument(
            "--fuzzy-match",
            action="store_true",
            help="Enable fuzzy matching for unresolved associated species names (slow).",
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
            "OccurrenceReportImporter (%s) started at %s (dry_run=%s)",
            self.slug,
            start_time.isoformat(),
            ctx.dry_run,
        )

        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        # Load pop_section_map early so we can use it for associated species filtering
        # CONDITIONAL LOADING: Only load TPFL specific maps if TPFL is in the sources
        # (This avoids loading large unnecessary files when running TEC migrations)
        pop_section_map = {}
        sheet_vws_map = {}
        if Source.TPFL.value in sources:
            pop_section_map = self.preload_pop_section_map(path)
            sheet_vws_map = self.preload_sheet_vws_map(path)

        tec_site_species_map = {}
        if Source.TEC_SITE_VISITS.value in sources:
            tec_site_species_map = self.preload_tec_site_species_map(path)

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []
        errors_details = []
        warnings_details = []

        # 1. Extract -- iterate adapters and accumulate rows while
        # emitting periodic progress so long-running extraction is visible.
        extracted = 0
        from django.utils.module_loading import import_string

        for src in sources:
            adapter = SOURCE_ADAPTERS[src]
            src_path = path_map.get(src, path)
            # adapter.extract may be expensive; log when each adapter completes
            logger.info(
                "OccurrenceReportImporter %s: extracting rows from source %s",
                self.slug,
                src,
            )
            # Lazily import the adapter class if it's a dotted path string.
            try:
                if isinstance(adapter, str):
                    adapter_cls = import_string(adapter)
                    # cache the class for later use (PIPELINES lookup)
                    SOURCE_ADAPTERS[src] = adapter_cls
                    adapter = adapter_cls
                # If we have a class, instantiate and cache the instance.
                if isinstance(adapter, type):
                    adapter_instance = adapter()
                    SOURCE_ADAPTERS[src] = adapter_instance
                    adapter = adapter_instance
            except Exception:
                logger.exception("Failed to prepare adapter for source %s", src)
                raise
            result = adapter.extract(src_path, **options)
            for w in result.warnings:
                warnings.append(f"{src}: {w.message}")
            # append rows one-by-one so we can log progress every N rows
            for r in result.rows:
                r["_source"] = src
                all_rows.append(r)
                extracted += 1
                if extracted % 500 == 0:
                    logger.info(
                        "OccurrenceReportImporter %s: extracted %d rows so far",
                        self.slug,
                        extracted,
                    )
        extract_end = timezone.now()
        extract_duration = extract_end - start_time
        logger.info(
            "OccurrenceReportImporter %s: extraction complete: %d rows extracted in %s",
            self.slug,
            extracted,
            str(extract_duration),
        )

        # Apply optional global per-importer limit (ctx.limit) after extraction
        limit = getattr(ctx, "limit", None)
        if limit:
            try:
                all_rows = all_rows[: int(limit)]
            except Exception:
                pass

        # 2. Build pipelines per-source by merging base schema pipelines with
        # adapter-provided `PIPELINES`. This keeps adapter-specific transforms
        # next to the adapter implementation while the importer runs them.
        from boranga.components.data_migration.registry import (
            registry as transform_registry,
        )

        base_column_names = schema.COLUMN_PIPELINES or {}
        pipelines_by_source: dict[str, dict] = {}
        for src_key, adapter in SOURCE_ADAPTERS.items():
            src_column_names = dict(base_column_names)
            adapter_pipes = getattr(adapter, "PIPELINES", None)
            if adapter_pipes:
                src_column_names.update(adapter_pipes)

            built: dict[str, list] = {}
            for col, names in src_column_names.items():
                built[col] = transform_registry.build_pipeline(names)
            pipelines_by_source[src_key] = built

        # Build a `pipelines` mapping (keys only) for merge/merge_group logic.
        all_columns = set()
        for built in pipelines_by_source.values():
            all_columns.update(built.keys())
        if not all_columns and schema.COLUMN_PIPELINES:
            all_columns.update(schema.COLUMN_PIPELINES.keys())
        pipelines = {col: None for col in sorted(all_columns)}

        # normalize_create_kwargs and apply_value_to_instance are provided
        # by the shared helpers module to avoid duplication across handlers.

        processed = 0
        transform_start = timezone.now()
        errors = 0
        created = 0
        updated = 0
        skipped = 0
        warn_count = 0

        # 3. Transform every row into canonical form, collect per-key groups
        groups: dict[str, list[tuple[dict, str, list[tuple[str, Any]]]]] = defaultdict(list)

        for row in all_rows:
            processed += 1
            if processed % 500 == 0:
                logger.info(
                    "OccurrenceReportImporter %s: processed %d rows so far",
                    self.slug,
                    processed,
                )

            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            # choose pipeline by row source
            src = row.get("_source")
            pipeline_map = pipelines_by_source.get(src, pipelines_by_source.get(None, {}))
            for col, pipeline in pipeline_map.items():
                raw_val = row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)
                transformed[col] = res.value
                for issue in res.issues:
                    issues.append((col, issue))
                    level = getattr(issue, "level", "error")
                    record = {
                        "migrated_from_id": row.get("migrated_from_id"),
                        "column": col,
                        "level": level,
                        "message": getattr(issue, "message", str(issue)),
                        "raw_value": raw_val,
                    }
                    if level == "error":
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
                    {
                        "reason": "missing_migrated_from_id",
                        "message": "missing_migrated_from_id",
                        "row": transformed,
                    }
                )
                continue
            groups[key].append((transformed, row.get("_source"), issues))

        # 4. Merge groups and persist one object per key
        def merge_group(entries, source_priority):
            entries_sorted = sorted(
                entries,
                key=lambda e: (source_priority.index(e[1]) if e[1] in source_priority else len(source_priority)),
            )
            merged = {}
            combined_issues = []
            # canonical columns
            for col in pipelines.keys():
                val = None
                for trans, src, _ in entries_sorted:
                    v = trans.get(col)
                    if v not in (None, ""):
                        val = v
                        break
                merged[col] = val
            # adapter-added extras
            extra_keys = set().union(*(set(trans.keys()) for trans, _, _ in entries_sorted))
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
            # Special-case: for OCRHabitatCondition percentage flags we want to
            # prefer the maximum non-empty numeric value across all entries.
            # The default merge above selects the first non-empty value which
            # can cause zeros from an earlier row to override a later 100%.
            for key in list(merged.keys()):
                if key.startswith("OCRHabitatCondition__"):
                    vals = []
                    for trans, _, _ in entries_sorted:
                        v = trans.get(key)
                        if v in (None, ""):
                            continue
                        try:
                            nv = int(v)
                        except Exception:
                            # ignore non-numeric values for the percentage flags
                            continue
                        vals.append(nv)
                    if vals:
                        merged[key] = max(vals)
                    else:
                        merged[key] = None
            for _, _, iss in entries_sorted:
                combined_issues.extend(iss)
            return merged, combined_issues

        # Persist merged rows in two phases to avoid N per-row DB ops (bulk_create/bulk_update)
        ops = []
        persisted = 0
        for migrated_from_id, entries in groups.items():
            persisted += 1
            if persisted % 500 == 0:
                logger.info(
                    "OccurrenceReportImporter %s: prepared %d groups so far",
                    self.slug,
                    persisted,
                )

            merged, combined_issues = merge_group(entries, sources)
            # skip if any error-level transform issues
            if any(i.level == "error" for _, i in combined_issues):
                skipped += 1
                continue

            # validate using schema's row dataclass if available
            report_row = None
            try:
                report_row = schema.OccurrenceReportRow.from_dict(merged)
                validation_issues = report_row.validate()
            except Exception as e:
                validation_issues = [("error", f"row_dataclass_error: {e}")]

            if validation_issues:
                for level, msg in validation_issues:
                    rec = {
                        "migrated_from_id": merged.get("migrated_from_id"),
                        "reason": "validation",
                        "level": level,
                        "message": str(msg),
                        "row": merged,
                    }
                    if level == "error":
                        errors_details.append(rec)
                    else:
                        warnings_details.append(rec)
                if any(level == "error" for level, _ in validation_issues):
                    skipped += 1
                    errors += sum(1 for level, _ in validation_issues if level == "error")
                    continue

            defaults = report_row.to_model_defaults()

            # Ensure `reported_date` is populated when missing by copying
            # from `lodgement_date`. The schema treats `reported_date` as a
            # copy of `lodgement_date` but the TPFL pipelines only produce
            # `lodgement_date`, so fill it here to avoid NULLs for the
            # model's non-nullable `reported_date` field.
            if defaults.get("reported_date") is None and defaults.get("lodgement_date") is not None:
                defaults["reported_date"] = defaults.get("lodgement_date")

            # If transforms produced None for fields that have model defaults
            # (for example CharFields with default=''), prefer the model's
            # default value. This keeps transforms simple (they can return
            # None) while avoiding validation failures for non-nullable
            # fields that expect a non-None default like an empty string.
            for k, v in list(defaults.items()):
                if v is not None:
                    continue
                try:
                    field = OccurrenceReport._meta.get_field(k)
                except FieldDoesNotExist:
                    continue
                # Prefer explicit field default (handles callables)
                field_default = field.get_default()
                if field_default is not None:
                    defaults[k] = field_default
                    continue
                # Fallback: for non-nullable text fields, prefer empty string
                if not getattr(field, "null", False) and isinstance(field, dj_models.CharField | dj_models.TextField):
                    defaults[k] = ""
                    continue

            if ctx.dry_run:
                # Avoid emitting extremely large JSON blobs to the logger which
                # can make the process appear to hang when many or very large
                # records are processed. Produce a truncated preview instead.
                try:
                    pretty = json.dumps(defaults, default=str, indent=2, sort_keys=True)
                    if len(pretty) > 2000:
                        preview = f"{pretty[:2000]}\n... (truncated, total {len(pretty)} chars)"
                    else:
                        preview = pretty
                except Exception:
                    # Fallback: build a concise summary of keys and value types
                    preview_items = []
                    for k, v in defaults.items():
                        sval = str(v)
                        if len(sval) > 200:
                            sval = sval[:200] + "..."
                        preview_items.append(f"{k}: {sval}")
                    preview = "\n".join(preview_items)

                logger.debug(
                    "OccurrenceReportImporter %s dry-run: would persist migrated_from_id=%s defaults (preview):\n%s",
                    self.slug,
                    migrated_from_id,
                    preview,
                )
                continue

            # capture related small extras for later (observer + habitat)
            # collect all OCRHabitatComposition__* keys into a habitat_data dict
            habitat_data = {}
            identification_data = {}
            habitat_condition = {}
            submitter_information_data = {}
            location_data = {}
            observation_detail_data = {}
            geometry_data = {}
            plant_count_data = {}
            animal_observation_data = {}
            vegetation_structure_data = {}
            fire_history_data = {}

            # Helper to extract .value from TransformResult if needed
            def extract_value(v):
                from boranga.components.data_migration.registry import TransformResult

                if isinstance(v, TransformResult):
                    return v.value
                return v

            for k, v in merged.items():
                if k.startswith("OCRHabitatComposition__"):
                    short = k.split("OCRHabitatComposition__", 1)[1]
                    habitat_data[short] = extract_value(v)
                if k.startswith("OCRHabitatCondition__"):
                    short = k.split("OCRHabitatCondition__", 1)[1]
                    habitat_condition[short] = extract_value(v)
                if k.startswith("OCRIdentification__"):
                    short = k.split("OCRIdentification__", 1)[1]
                    identification_data[short] = extract_value(v)
                if k.startswith("SubmitterInformation__"):
                    short = k.split("SubmitterInformation__", 1)[1]
                    submitter_information_data[short] = extract_value(v)
                if k.startswith("OCRLocation__"):
                    short = k.split("OCRLocation__", 1)[1]
                    location_data[short] = extract_value(v)
                if k.startswith("OCRObservationDetail__"):
                    short = k.split("OCRObservationDetail__", 1)[1]
                    observation_detail_data[short] = extract_value(v)
                if k.startswith("OccurrenceReportGeometry__"):
                    short = k.split("OccurrenceReportGeometry__", 1)[1]
                    geometry_data[short] = extract_value(v)
                if k.startswith("OCRPlantCount__"):
                    short = k.split("OCRPlantCount__", 1)[1]
                    plant_count_data[short] = extract_value(v)
                if k.startswith("OCRAnimalObservation__"):
                    short = k.split("OCRAnimalObservation__", 1)[1]
                    animal_observation_data[short] = extract_value(v)
                if k.startswith("OCRVegetationStructure__"):
                    short = k.split("OCRVegetationStructure__", 1)[1]
                    vegetation_structure_data[short] = extract_value(v)
                if k.startswith("OCRFireHistory__"):
                    short = k.split("OCRFireHistory__", 1)[1]
                    fire_history_data[short] = extract_value(v)

            ops.append(
                {
                    "migrated_from_id": migrated_from_id,
                    "canonical": report_row,
                    "defaults": defaults,
                    "merged": merged,
                    "habitat_data": habitat_data,
                    "habitat_condition": habitat_condition,
                    "identification_data": identification_data,
                    "submitter_information_data": submitter_information_data,
                    "location_data": location_data,
                    "observation_detail_data": observation_detail_data,
                    "geometry_data": geometry_data,
                    "plant_count_data": plant_count_data,
                    "animal_observation_data": animal_observation_data,
                    "vegetation_structure_data": vegetation_structure_data,
                    "fire_history_data": fire_history_data,
                }
            )

        transform_end = timezone.now()
        transform_duration = transform_end - transform_start
        logger.info(
            "OccurrenceReportImporter %s: transform phase complete (groups=%d) in %s",
            self.slug,
            len(ops),
            str(transform_duration),
        )

        # Pre-fetch Occurrences for linking
        def get_occ_lookup_id(report_mig_id, raw_occ_id):
            if not raw_occ_id:
                return None

            # If raw_occ_id is an Occurrence object (returned by occurrence_from_pop_id),
            # use its migrated_from_id string.
            if hasattr(raw_occ_id, "migrated_from_id"):
                return raw_occ_id.migrated_from_id

            for src in SOURCE_ADAPTERS.keys():
                prefix = f"{src.lower()}-"
                if report_mig_id.startswith(prefix):
                    if not raw_occ_id.startswith(prefix):
                        return f"{prefix}{raw_occ_id}"
            return raw_occ_id

        occ_mig_ids = set()
        for op in ops:
            row = op["canonical"]
            occ_link = row.Occurrence__migrated_from_id

            # Fallback for TPFL if linkage failed via standard mapping
            if not occ_link and op["migrated_from_id"].startswith("tpfl-"):
                sheet_no = op["migrated_from_id"].split("-", 1)[1]
                pop_id = sheet_vws_map.get(sheet_no.casefold())
                if pop_id:
                    # In Boranga, TPFL Occurrences use 'tpfl-' prefix in migrated_from_id
                    occ_mig_id = f"tpfl-{pop_id}"
                    occ_link = occ_mig_id
                    # Update canonical row so subsequent logic finds it
                    row.Occurrence__migrated_from_id = occ_mig_id

            lid = get_occ_lookup_id(op["migrated_from_id"], occ_link)
            if lid:
                occ_mig_ids.add(lid)

        occ_map = {}
        if occ_mig_ids:
            occ_map = {o.migrated_from_id: o for o in Occurrence.objects.filter(migrated_from_id__in=occ_mig_ids)}

        for op in ops:
            row = op["canonical"]
            defaults = op["defaults"]

            # Resolve occurrence link and copy details
            if row.Occurrence__migrated_from_id:
                lid = get_occ_lookup_id(op["migrated_from_id"], row.Occurrence__migrated_from_id)
                occ = occ_map.get(lid)
                if occ:
                    # Replace string mapping with ID
                    defaults["occurrence_id"] = occ.id
                    defaults.pop("occurrence", None)

                    # Copy name and number if not present (TEC requirement)
                    if not defaults.get("ocr_for_occ_name"):
                        defaults["ocr_for_occ_name"] = occ.occurrence_name
                    if not defaults.get("ocr_for_occ_number"):
                        defaults["ocr_for_occ_number"] = occ.occurrence_number

                    # Copy community from parent occurrence (Tasks 12299, 12531)
                    if occ.community_id and not defaults.get("community_id"):
                        defaults["community_id"] = occ.community_id

                    # Copy Location info from Parent Occurrence (Tasks 12349, 12352, 12357)
                    loc_data = op["location_data"]
                    occ_loc = getattr(occ, "location", None)
                    if occ_loc:
                        if not loc_data.get("district") and occ_loc.district_id:
                            loc_data["district"] = occ_loc.district_id
                        if not loc_data.get("region") and occ_loc.region_id:
                            loc_data["region"] = occ_loc.region_id
                        if not loc_data.get("location_description") and occ_loc.location_description:
                            loc_data["location_description"] = occ_loc.location_description

            # Cleanup: If we failed to link an occurrence, ensure we don't pass the raw string ID
            # (e.g. "1993") as a PK to Django, which causes ForeignKey Violations.
            # We log a warning if we are dropping a real value so the data gap is visible.
            if "occurrence" in defaults:
                removed_val = defaults.pop("occurrence")
                if removed_val and not defaults.get("occurrence_id"):
                    # Only log provided we haven't already linked it (occurrence_id)
                    errors_details.append(
                        {
                            "migrated_from_id": op["migrated_from_id"],
                            "column": "occurrence",
                            "level": "warning",
                            "message": f"Link broken. Report refers to Occurrence '{removed_val}' which was not found in DB. Creating unlinked.",
                            "raw_value": removed_val,
                            "reason": "broken_link",
                            "row": {"occurrence_link": removed_val},
                        }
                    )

        # Build op_map for O(1) access to per-migrated-id data (avoid O(n) scans)
        op_map = {o["migrated_from_id"]: o for o in ops}

        # Prefetch existing OccurrenceReports to decide create vs update
        migrated_keys = [o["migrated_from_id"] for o in ops]
        existing_by_migrated = {
            s.migrated_from_id: s
            for s in OccurrenceReport.objects.filter(migrated_from_id__in=migrated_keys).select_related("occurrence")
        }

        # Prepare lists for bulk ops
        to_create = []
        create_meta = []
        to_update = []
        BATCH = 1000

        for op in ops:
            migrated_from_id = op["migrated_from_id"]
            defaults = op["defaults"]
            habitat_data = op.get("habitat_data") or {}
            habitat_condition = op.get("habitat_condition") or {}
            submitter_information_data = op.get("submitter_information_data") or {}
            location_data = op.get("location_data") or {}
            observation_detail_data = op.get("observation_detail_data") or {}
            geometry_data = op.get("geometry_data") or {}
            plant_count_data = op.get("plant_count_data") or {}
            vegetation_structure_data = op.get("vegetation_structure_data") or {}
            fire_history_data = op.get("fire_history_data") or {}

            obj = existing_by_migrated.get(migrated_from_id)
            if obj:
                # apply defaults to instance for later bulk_update
                for k, v in defaults.items():
                    apply_value_to_instance(obj, k, v)
                to_update.append(
                    (
                        obj,
                        habitat_data,
                        habitat_condition,
                        submitter_information_data,
                        location_data,
                        observation_detail_data,
                        geometry_data,
                        plant_count_data,
                        vegetation_structure_data,
                        fire_history_data,
                    )
                )
                continue

            # create new instance (bulk_create later)
            create_kwargs = dict(defaults)
            create_kwargs["migrated_from_id"] = migrated_from_id
            if getattr(ctx, "migration_run", None) is not None:
                create_kwargs["migration_run"] = ctx.migration_run
            inst = OccurrenceReport(**normalize_create_kwargs(OccurrenceReport, create_kwargs))
            to_create.append(inst)
            create_meta.append(
                (
                    migrated_from_id,
                    habitat_data,
                    habitat_condition,
                    submitter_information_data,
                    location_data,
                    observation_detail_data,
                    geometry_data,
                    plant_count_data,
                    vegetation_structure_data,
                    fire_history_data,
                )
            )

        # Bulk create new OccurrenceReports
        created_map = {}
        if to_create:
            logger.info(
                "OccurrenceReportImporter: bulk-creating %d new OccurrenceReports",
                len(to_create),
            )
            for i in range(0, len(to_create), BATCH):
                chunk = to_create[i : i + BATCH]
                with transaction.atomic():
                    OccurrenceReport.objects.bulk_create(chunk, batch_size=BATCH)

        # Refresh created objects to get PKs
        if create_meta:
            created_keys = [m[0] for m in create_meta]
            # logger.info(f"created_keys for lookup: {created_keys[:5]}")
            for s in OccurrenceReport.objects.filter(migrated_from_id__in=created_keys).select_related("occurrence"):
                # logger.info(f"Adding to created_map: {s.migrated_from_id}={s.pk}")
                created_map[s.migrated_from_id] = s

        # Populate occurrence_report_number for newly-created objects (bulk_update)
        if created_map:
            occs_to_update = []
            for mig, s in created_map.items():
                if not s.occurrence_report_number:
                    s.occurrence_report_number = f"{s.MODEL_PREFIX}{s.pk}"
                    occs_to_update.append(s)
            if occs_to_update:
                try:
                    OccurrenceReport.objects.bulk_update(occs_to_update, ["occurrence_report_number"], batch_size=BATCH)
                except Exception:
                    for s in occs_to_update:
                        try:
                            s.save()
                        except Exception:
                            logger.exception(
                                "Failed to populate occurrence_report_number for created OccurrenceReport %s",
                                getattr(s, "pk", None),
                            )

        # Bulk update existing objects
        if to_update:
            logger.info(
                "OccurrenceReportImporter: bulk-updating %d existing OccurrenceReports",
                len(to_update),
            )
            update_instances = [t[0] for t in to_update]
            # determine fields to update: include only fields that are
            # non-None on every instance. Using the union (fields present on
            # some instances) can cause bulk_update to write NULL into rows
            # for instances where the attribute is None, which violates NOT
            # NULL constraints (e.g. `reported_date`). Restricting to fields
            # present on all instances avoids that.
            fields = []
            if update_instances:
                all_fields = [f for f in update_instances[0]._meta.fields]
                for f in all_fields:
                    if f.name in ("id", "migrated_from_id"):
                        continue
                    # include field only if every instance has a non-None value
                    try:
                        if f.null or all(getattr(inst, f.name, None) is not None for inst in update_instances):
                            fields.append(f.name)
                    except Exception:
                        # Be conservative: skip fields that raise on getattr
                        continue
            # perform bulk_update only if we have safe fields to update
            try:
                if fields:
                    OccurrenceReport.objects.bulk_update(update_instances, fields, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OccurrenceReport; falling back to individual saves")
                for inst in update_instances:
                    try:
                        # Build a conservative per-instance update_fields list:
                        # include only model fields that currently have a non-None
                        # value on the instance. This avoids attempting to write
                        # NULL into non-nullable DB columns such as
                        # `reported_date` when the instance attribute is None.
                        update_fields = [
                            f.name
                            for f in inst._meta.fields
                            if getattr(inst, f.name, None) is not None and f.name not in ("id", "migrated_from_id")
                        ]
                        if update_fields:
                            inst.save(update_fields=update_fields)
                        else:
                            # Nothing to update (all values are None or only PK), skip
                            logger.debug(
                                "Skipping save for OccurrenceReport %s: no updatable fields",
                                getattr(inst, "pk", None),
                            )
                    except Exception:
                        logger.exception(
                            "Failed to save OccurrenceReport %s",
                            getattr(inst, "pk", None),
                        )

        # Now handle related models in bulk for both created and updated occurrence reports
        # Prepare target occurrence_report ids
        target_mig_ids = [o["migrated_from_id"] for o in ops]
        target_occs = list(OccurrenceReport.objects.filter(migrated_from_id__in=target_mig_ids))
        target_map = {o.migrated_from_id: o for o in target_occs}

        # Load associated-species mapping (SHEETNO -> [species names]) from
        # mappings module. The loader will look for
        # DRF_SHEET_VEG_CLASSES_Ass_species.csv alongside the provided `path`.
        # During dry-run, load a small sample and produce a concise debug
        # preview instead of performing full DB resolution/creation.
        # During dry-run we already emit a per-OCR associated-species preview
        # immediately after each OCR defaults preview above. To avoid running
        # the aggregated (and potentially expensive) sheet-level summary and
        # duplicate logs, skip loading the full mapping in dry-run mode.
        if getattr(ctx, "dry_run", False):
            sheet_to_species = None
        else:
            sheet_to_species = load_sheet_associated_species_names(path, split_values=True)

        # If any mapping rows found, resolve names to AssociatedSpeciesTaxonomy
        # Also, scan OCRAssociatedSpecies__comment for additional species names
        # and merge them into sheet_to_species.

        # 0. Initialize normalized mapping from loaded file
        normalized_sheet_to_species: dict[str, list[str]] = {}
        if sheet_to_species:
            for k, v in sheet_to_species.items():
                if k is None:
                    continue
                ks = str(k).strip()
                if not ks:
                    continue
                normalized_sheet_to_species[ks] = [str(n).strip() for n in v if n]

        sheet_to_species = normalized_sheet_to_species

        # 1. Scan ops for OCRAssociatedSpecies__comment and extract names
        import re

        extra_species_count = 0
        for op in ops:
            mig = op["migrated_from_id"]
            if not mig:
                continue

            merged_dict = op.get("merged") or {}
            comment = merged_dict.get("OCRAssociatedSpecies__comment")

            if comment and str(comment).strip():
                # Split by comma or semicolon
                raw_names = re.split(r"[,;]+", str(comment))
                extracted = [n.strip() for n in raw_names if n.strip()]

                if extracted:
                    if mig not in sheet_to_species:
                        sheet_to_species[mig] = []

                    # Add newly found names if not already present
                    current_set = {n.casefold() for n in sheet_to_species[mig]}
                    for name in extracted:
                        if name.casefold() not in current_set:
                            sheet_to_species[mig].append(name)
                            current_set.add(name.casefold())
                            extra_species_count += 1

        logger.info(
            "OccurrenceReportImporter: Extracted %d additional associated species references from comments",
            extra_species_count,
        )

        if sheet_to_species:
            # Normalize sheet keys to strings and strip; ensure matching with
            # target_map keys which are strings from migrated_from_id.
            # (already done in step 0, but sheet_to_species was mutated in step 1,
            # so keys are already consistent. Just proceed.)

            # unique species names
            uniq_names = {n for lst in sheet_to_species.values() for n in lst}

            logger.info(
                "OccurrenceReportImporter: resolving %d unique associated-species names",
                len(uniq_names),
            )

            # Batch-resolve Taxonomy by case-insensitive scientific_name.
            # Use a server-side array join (unnest) to avoid huge IN(...) lists
            # which are slow to plan/parse and may hit driver/param limits.

            # Normalize names client-side to match lower(...) on DB.
            lower_names = {str(n).strip().casefold() for n in uniq_names if n and str(n).strip()}
            lower_names = list(lower_names)

            # 1. Resolve name -> taxonomy via Legacy Mapping if exists (highest priority)
            legacy_mappings = {
                m.legacy_canonical_name: m.taxonomy
                for m in LegacyTaxonomyMapping.objects.filter(
                    list_name="TPFL AssociatedSpecies",
                    legacy_canonical_name__in=uniq_names,
                    taxonomy__isnull=False,
                )
            }

            taxa_map = {}
            ambiguous_species = {}
            if lower_names:
                # Resolve table and index names
                from django.db.models.functions import Lower

                # Fetch all matching Taxonomies (including historical ones)
                # Group by scientific_name (lowercase) for correct duplicate handling
                # We need to fetch: id, scientific_name, and other fields to determine "current"
                # Assuming there's a way to determine "current" (e.g. no end_date or similar concept)
                # But here the requirement is:
                # 1. If not found -> warning
                # 2. If multiple found -> prefer is_current=True
                # 3. If multiple is_current=True -> warning
                # We'll do this in Python to handle the complex logic, but batch-fetch carefully.
                # Since we have duplicate names in DB potentially, unnest is tricky if it returns partial matches.
                # Let's use the IN-clause with batching, but selecting *all* matches.

                matches_by_name = defaultdict(list)

                batch_size = 2000
                for i in range(0, len(lower_names), batch_size):
                    batch = lower_names[i : i + batch_size]
                    qs = Taxonomy.objects.annotate(lname=Lower("scientific_name")).filter(lname__in=batch)
                    # We might need to select is_cal_name or similar if that indicates "current"?
                    # Checking usage: TaxonPreviousName suggests Taxonomy might not have is_current flag directly?
                    # Let's check the model first in a separate step or assume a field exists.
                    # Wait, user prompt mentioned "is_current=True". Assuming Taxonomy has is_current.
                    for t in qs:
                        key = t.scientific_name.strip().casefold()
                        # Avoid duplicates if batching somehow fetches same obj twice (defensive)
                        if t not in matches_by_name[key]:
                            matches_by_name[key].append(t)

                # Now resolve "best" match per name
                for name in uniq_names:
                    # If we have a legacy mapping, we don't need to resolve via scientific name
                    # and we definitely don't want to warn about ambiguity for this name.
                    if name in legacy_mappings:
                        continue

                    lname = name.strip().casefold()
                    candidates = matches_by_name.get(lname, [])

                    if not candidates:
                        # Case 1: Not found -> Log warning
                        # warnings.append(
                        #    f"Associated Species Taxonomy not found: {name}"
                        # )
                        continue

                    if len(candidates) == 1:
                        taxa_map[lname] = candidates[0]
                        continue

                    # Multiple candidates found
                    current_candidates = [c for c in candidates if getattr(c, "is_current", False)]

                    if len(current_candidates) == 1:
                        # Case 2: Exactly one active
                        taxa_map[lname] = current_candidates[0]
                    elif len(current_candidates) > 1:
                        # Case 3: Multiple active -> Record for ambiguity warning later
                        ambiguous_species[name] = [c.pk for c in current_candidates]
                        # Fallback: maybe pick the one with highest ID? or latest created?
                        # For now, just pick the first one to allow migration to proceed, but warned.
                        taxa_map[lname] = current_candidates[0]
                    else:
                        # None are current, but we have candidates. Pick the first one (historical?)
                        # Might want to warn here too? "Multiple candidates but none current".
                        # Let's silently pick the last one (highest ID typically) or just first.
                        # Sort by ID descending to get "newest" record effectively
                        candidates.sort(key=lambda x: x.pk, reverse=True)
                        taxa_map[lname] = candidates[0]

            name_to_tax: dict[str, Taxonomy] = {}
            unresolved = []
            best_guess_map = {}
            for name in uniq_names:
                # 1. Try Legacy Mapping first (exact match)
                tax = legacy_mappings.get(name)

                # 2. Try case-insensitive scientific name match
                if not tax:
                    ln = name.casefold()
                    tax = taxa_map.get(ln)

                if tax:
                    name_to_tax[name] = tax
                else:
                    unresolved.append(name)

            # 3. Best-guess fuzzy matching for unresolved names
            if unresolved and options.get("fuzzy_match"):
                still_unresolved = []
                for name in unresolved:
                    if len(name) < 5:
                        still_unresolved.append(name)
                        continue

                    parts = name.split()
                    genus = parts[0] if parts else None
                    if not genus:
                        still_unresolved.append(name)
                        continue

                    # Try to find candidates sharing the same genus
                    candidates = list(
                        Taxonomy.objects.filter(is_current=True, genera_name__iexact=genus)
                        .values_list("scientific_name", flat=True)
                        .order_by("scientific_name")
                    )

                    if not candidates:
                        # Fallback try startswith
                        candidates = list(
                            Taxonomy.objects.filter(is_current=True, scientific_name__istartswith=genus)
                            .values_list("scientific_name", flat=True)
                            .order_by("scientific_name")
                        )

                    if candidates:
                        matches = difflib.get_close_matches(name, candidates, n=1, cutoff=0.85)
                        if matches:
                            guess_name = matches[0]
                            tax = Taxonomy.objects.filter(is_current=True, scientific_name=guess_name).first()
                            if tax:
                                name_to_tax[name] = tax
                                best_guess_map[name] = tax
                                continue

                    still_unresolved.append(name)
                unresolved = still_unresolved

            if best_guess_map:
                # Build mapping: raw_sheet_no -> [list of best guesses on this sheet]
                sheet_to_guesses = defaultdict(list)
                for sheet_no, sp_list in sheet_to_species.items():
                    guesses_on_sheet = [s for s in sp_list if s in best_guess_map]
                    if guesses_on_sheet:
                        sheet_to_guesses[str(sheet_no)] = guesses_on_sheet

                # Check active import groups for matches
                for migrated_from_id in groups:
                    if "-" in migrated_from_id:
                        suffix = migrated_from_id.split("-", 1)[1]
                    else:
                        suffix = migrated_from_id

                    if suffix in sheet_to_guesses:
                        for raw_name in sheet_to_guesses[suffix]:
                            tax = best_guess_map[raw_name]
                            errors_details.append(
                                {
                                    "migrated_from_id": migrated_from_id,
                                    "column": "associated_species",
                                    "level": "warning",
                                    "message": (
                                        f"Best-guess taxonomy match for '{raw_name}': using '{tax.scientific_name}'"
                                    ),
                                    "raw_value": raw_name,
                                    "reason": "associated_species_best_guess",
                                    "row": {},
                                    "timestamp": timezone.now().isoformat(),
                                }
                            )
                            # We don't necessarily want to treat this as a high-priority warning
                            # but it's good to have in the report.
                            warn_count += 1

            if ambiguous_species:
                # Build mapping: raw_sheet_no -> [list of ambiguous species on this sheet]
                sheet_to_ambiguous_species = defaultdict(list)
                for sheet_no, sp_list in sheet_to_species.items():
                    amb_on_sheet = [s for s in sp_list if s in ambiguous_species]
                    if amb_on_sheet:
                        sheet_to_ambiguous_species[str(sheet_no)] = amb_on_sheet

                # Check active import groups for matches
                for migrated_from_id in groups:
                    # Heuristic: migrated_from_id is {prefix}-{sheet_no}
                    # or just {sheet_no}
                    if "-" in migrated_from_id:
                        suffix = migrated_from_id.split("-", 1)[1]
                    else:
                        suffix = migrated_from_id

                    if suffix in sheet_to_ambiguous_species:
                        for amb_sp in sheet_to_ambiguous_species[suffix]:
                            cands = ambiguous_species.get(amb_sp, [])
                            errors_details.append(
                                {
                                    "migrated_from_id": migrated_from_id,
                                    "column": "associated_species",
                                    "level": "warning",
                                    "message": f"Multiple current Taxonomy records found for '{amb_sp}': {cands}",
                                    "raw_value": amb_sp,
                                    "reason": "associated_species_ambiguity",
                                    "row": {},
                                    "timestamp": timezone.now().isoformat(),
                                }
                            )
                            warn_count += 1

            if unresolved:
                logger.warning(
                    "OccurrenceReportImporter: %d associated-species names unresolved",
                    len(unresolved),
                )

                # Build mapping: raw_sheet_no -> [list of unresolved species on this sheet]
                sheet_to_bad_species = defaultdict(list)
                unresolved_set = set(unresolved)
                for sheet_no, sp_list in sheet_to_species.items():
                    bad_on_sheet = [s for s in sp_list if s in unresolved_set]
                    if bad_on_sheet:
                        sheet_to_bad_species[str(sheet_no)] = bad_on_sheet

                # Check active import groups for matches
                assoc_warnings_count = 0
                for migrated_from_id in groups:
                    # Heuristic: migrated_from_id is {prefix}-{sheet_no}
                    # or just {sheet_no} depending on source.
                    # We try to extract suffix if a dash is present.
                    if "-" in migrated_from_id:
                        suffix = migrated_from_id.split("-", 1)[1]
                    else:
                        suffix = migrated_from_id

                    if suffix in sheet_to_bad_species:
                        for bad_sp in sheet_to_bad_species[suffix]:
                            errors_details.append(
                                {
                                    "migrated_from_id": migrated_from_id,
                                    "column": "associated_species",
                                    "level": "warning",
                                    "message": f"no taxonomy match for '{bad_sp}'",
                                    "raw_value": bad_sp,
                                    "reason": "associated_species_resolution",
                                    "row": {},
                                    "timestamp": timezone.now().isoformat(),
                                }
                            )
                            warn_count += 1
                            assoc_warnings_count += 1

                # If we have unresolved species but no matching rows in the current import
                # (e.g. data filtered out), we still record a generic warning to avoid silence.
                if assoc_warnings_count == 0:
                    for ex in unresolved[:20]:
                        warnings.append(
                            f"associated_species: no taxonomy match for '{ex}' (no matching sheet imported)"
                        )
                else:
                    logger.info(
                        "Generated %d row-specific warnings for associated species",
                        assoc_warnings_count,
                    )

            # Load existing AssociatedSpeciesTaxonomy rows for all resolved taxonomy ids
            tax_ids = {t.pk for t in name_to_tax.values()}
            ast_qs = AssociatedSpeciesTaxonomy.objects.filter(taxonomy__in=list(tax_ids))
            # Map taxonomy_id -> AssociatedSpeciesTaxonomy (take first if multiple)
            taxid_to_ast = {}
            for ast in ast_qs:
                if ast.taxonomy_id not in taxid_to_ast:
                    taxid_to_ast[ast.taxonomy_id] = ast
            # Create missing AST rows for taxonomy ids that have none
            missing_tax_ids = tax_ids - set(taxid_to_ast.keys())
            if missing_tax_ids:
                # Create missing AssociatedSpeciesTaxonomy rows in bulk to
                # avoid per-id DB roundtrips. Fall back to individual creates
                # if bulk_create fails for any reason.
                try:
                    create_objs = [AssociatedSpeciesTaxonomy(taxonomy_id=tid) for tid in missing_tax_ids]
                    AssociatedSpeciesTaxonomy.objects.bulk_create(create_objs, batch_size=BATCH)
                    # Refresh created rows to ensure we have their PKs
                    for ast in AssociatedSpeciesTaxonomy.objects.filter(taxonomy_id__in=list(missing_tax_ids)):
                        if ast.taxonomy_id not in taxid_to_ast:
                            taxid_to_ast[ast.taxonomy_id] = ast
                except Exception:
                    logger.exception("Bulk create failed for AssociatedSpeciesTaxonomy; trying individual creates")
                    created_asts = []
                    for tid in missing_tax_ids:
                        try:
                            created_asts.append(AssociatedSpeciesTaxonomy.objects.create(taxonomy_id=tid))
                        except Exception:
                            logger.exception(
                                "Failed to create AssociatedSpeciesTaxonomy for taxonomy_id %s",
                                tid,
                            )
                    for ast in created_asts:
                        taxid_to_ast[ast.taxonomy_id] = ast

            # Build final name -> ast mapping
            name_to_assoc: dict[str, AssociatedSpeciesTaxonomy] = {}
            for name, tax in name_to_tax.items():
                ast = taxid_to_ast.get(tax.pk)
                if ast:
                    name_to_assoc[name] = ast

            # Fetch existing OCRAssociatedSpecies for target occs; prefetch
            # related_species to avoid per-object queries later.
            existing_assoc = {
                a.occurrence_report_id: a
                for a in OCRAssociatedSpecies.objects.filter(occurrence_report__in=target_occs).prefetch_related(
                    "related_species"
                )
            }

            # Create OCRAssociatedSpecies for occurrence reports that need them
            assoc_to_create = []
            ocr_id_to_resolved = {}  # Store resolved species for M2M population

            # Iterate over all target occurrence reports, not just those in sheet_to_species
            for sheetno, ocr in target_map.items():
                if ocr.pk in existing_assoc:
                    continue

                # Check if we have species
                names = sheet_to_species.get(sheetno, [])
                resolved = [name_to_assoc[n] for n in names if n in name_to_assoc]

                # Check if we have comment
                op = op_map.get(sheetno)
                comment = None
                if op:
                    merged = op.get("merged") or {}
                    comment = merged.get("OCRAssociatedSpecies__comment")

                if resolved:
                    ocr_id_to_resolved[ocr.pk] = resolved

                if resolved or comment:
                    assoc = OCRAssociatedSpecies(occurrence_report=ocr)
                    if comment:
                        assoc.comment = comment
                    assoc_to_create.append(assoc)

            if assoc_to_create:
                try:
                    OCRAssociatedSpecies.objects.bulk_create(assoc_to_create, batch_size=BATCH)

                    # Populate ManyToMany relations for newly created objects
                    # Use through model for bulk creation to avoid N+1 queries
                    ThroughModel = OCRAssociatedSpecies.related_species.through
                    m2m_links = []

                    # Fetch them back to ensure we have IDs
                    created_assocs = OCRAssociatedSpecies.objects.filter(
                        occurrence_report_id__in=list(ocr_id_to_resolved.keys())
                    )
                    for assoc in created_assocs:
                        r_list = ocr_id_to_resolved.get(assoc.occurrence_report_id)
                        if r_list:
                            # Deduplicate taxonomy objects to avoid IntegrityError on bulk_create
                            unique_tax_ids = set()
                            for tax_obj in r_list:
                                if tax_obj.id in unique_tax_ids:
                                    continue
                                unique_tax_ids.add(tax_obj.id)
                                m2m_links.append(
                                    ThroughModel(
                                        ocrassociatedspecies_id=assoc.id,
                                        associatedspeciestaxonomy_id=tax_obj.id,
                                    )
                                )

                    if m2m_links:
                        ThroughModel.objects.bulk_create(m2m_links, batch_size=BATCH)

                except Exception:
                    logger.exception("Failed to bulk_create OCRAssociatedSpecies; falling back to individual saves")
                    for a in assoc_to_create:
                        try:
                            a.save()
                            # Also populate M2M on fallback
                            if a.occurrence_report_id in ocr_id_to_resolved:
                                a.related_species.add(*ocr_id_to_resolved[a.occurrence_report_id])
                        except Exception as exc:
                            logger.exception(
                                "Failed to create OCRAssociatedSpecies for occurrence_report %s",
                                getattr(a.occurrence_report, "pk", None),
                            )
                            ocr_ref = getattr(a, "occurrence_report", None)
                            errors_details.append(
                                {
                                    "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                    "column": "OCRAssociatedSpecies",
                                    "level": "error",
                                    "message": f"Failed to create associated species: {exc}",
                                    "raw_value": "",
                                    "reason": "create_error",
                                    "row": {"pk": getattr(ocr_ref, "pk", "")},
                                    "timestamp": timezone.now().isoformat(),
                                }
                            )
                            errors += 1

            # Update existing OCRAssociatedSpecies with comments
            assoc_to_update = []
            for sheetno, ocr in target_map.items():
                if ocr.pk not in existing_assoc:
                    continue

                assoc = existing_assoc[ocr.pk]
                op = op_map.get(sheetno)
                if op:
                    merged = op.get("merged") or {}
                    comment = merged.get("OCRAssociatedSpecies__comment")
                    if comment and assoc.comment != comment:
                        assoc.comment = comment
                        assoc_to_update.append(assoc)

            if assoc_to_update:
                try:
                    OCRAssociatedSpecies.objects.bulk_update(assoc_to_update, ["comment"], batch_size=BATCH)
                except Exception:
                    logger.exception(
                        "Failed to bulk_update OCRAssociatedSpecies comments; falling back to individual saves"
                    )
                for a in assoc_to_update:
                    try:
                        a.save(update_fields=["comment"])
                    except Exception as exc:
                        logger.exception(
                            "Failed to update OCRAssociatedSpecies %s",
                            getattr(a, "pk", None),
                        )
                        ocr_ref = getattr(a, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRAssociatedSpecies",
                                "level": "error",
                                "message": f"Failed to update associated species comment: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(a, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1
            through = OCRAssociatedSpecies.related_species.through
            assoc_fk_field = None
            tax_fk_field = None
            for f in through._meta.get_fields():
                if getattr(f, "remote_field", None) and getattr(f.remote_field, "model", None) == OCRAssociatedSpecies:
                    assoc_fk_field = f.name
                if (
                    getattr(f, "remote_field", None)
                    and getattr(f.remote_field, "model", None) == AssociatedSpeciesTaxonomy
                ):
                    tax_fk_field = f.name
            if assoc_fk_field and tax_fk_field:
                assoc_fk_id = assoc_fk_field + "_id"
                tax_fk_id = tax_fk_field + "_id"

                to_create_through = []
                to_delete_filters = []
                for sheetno, names in sheet_to_species.items():
                    ocr = target_map.get(sheetno)
                    if not ocr:
                        continue
                    assoc_obj = existing_assoc.get(ocr.pk)
                    if not assoc_obj:
                        continue
                    desired_ids = {name_to_assoc[n].pk for n in names if n in name_to_assoc}
                    # existing related ids (prefetched so no DB hit per-obj)
                    existing_ids = {s.pk for s in assoc_obj.related_species.all()}
                    add_ids = desired_ids - existing_ids
                    remove_ids = existing_ids - desired_ids
                    for aid in add_ids:
                        to_create_through.append(through(**{assoc_fk_id: assoc_obj.pk, tax_fk_id: aid}))
                    if remove_ids:
                        to_delete_filters.append(
                            {
                                assoc_fk_id: assoc_obj.pk,
                                tax_fk_id + "__in": list(remove_ids),
                            }
                        )

                # perform deletes
                for f in to_delete_filters:
                    try:
                        through.objects.filter(**f).delete()
                    except Exception:
                        logger.exception(
                            "Failed to delete old associated-species through rows: %s",
                            f,
                        )

                # perform bulk create for new through rows (in chunks)
                if to_create_through:
                    try:
                        for i in range(0, len(to_create_through), BATCH):
                            through.objects.bulk_create(to_create_through[i : i + BATCH], batch_size=BATCH)
                    except Exception:
                        logger.exception(
                            "Failed to bulk_create associated-species through rows; falling back to individual saves"
                        )
                        for t in to_create_through:
                            try:
                                t.save()
                            except Exception:
                                logger.exception(
                                    "Failed to create through row for OCRAssociatedSpecies %s",
                                    getattr(t, assoc_fk_id, None),
                                )

                # If an Occurrence is linked to the OccurrenceReport, duplicate
                # the AssociatedSpeciesTaxonomy rows so the OCC (Occurrence) gets
                # its own per-association records. Use bulk operations where
                # possible: bulk-create any missing OCCAssociatedSpecies, bulk
                # create AssociatedSpeciesTaxonomy duplicates with a unique
                # temporary marker in `comments` to map them back, bulk-create
                # through rows linking OCCAssociatedSpecies -> new ASTs, then
                # clean up the temporary markers.
                try:
                    # target_occs is a list of OccurrenceReport instances we loaded earlier
                    occ_reports_with_occ = [o for o in target_occs if getattr(o, "occurrence_id", None)]
                    if occ_reports_with_occ:
                        from boranga.components.occurrence.models import (
                            AssociatedSpeciesTaxonomy as _AST,
                        )

                        # Build set of occurrence ids
                        occ_ids = {o.occurrence_id for o in occ_reports_with_occ}

                        # Ensure OCCAssociatedSpecies exists for each occurrence (bulk-create missing)
                        existing_occ_assoc = {
                            a.occurrence_id: a
                            for a in OCCAssociatedSpecies.objects.filter(occurrence_id__in=list(occ_ids))
                        }
                        occ_assoc_to_create = []
                        for o in occ_reports_with_occ:
                            occ = getattr(o, "occurrence", None)
                            if not occ:
                                continue
                            if occ.id not in existing_occ_assoc:
                                occ_assoc_to_create.append(OCCAssociatedSpecies(occurrence=occ))

                        if occ_assoc_to_create:
                            try:
                                OCCAssociatedSpecies.objects.bulk_create(occ_assoc_to_create, batch_size=BATCH)
                            except Exception:
                                logger.exception(
                                    "Failed to bulk_create OCCAssociatedSpecies; falling back to individual creates"
                                )
                                for a in occ_assoc_to_create:
                                    try:
                                        a.save()
                                    except Exception:
                                        logger.exception(
                                            "Failed to create OCCAssociatedSpecies for occurrence %s",
                                            getattr(a.occurrence, "pk", None),
                                        )

                        # Refresh mapping
                        existing_occ_assoc = {
                            a.occurrence_id: a
                            for a in OCCAssociatedSpecies.objects.filter(occurrence_id__in=list(occ_ids))
                        }

                        # Prepare duplicates to create: list of (occ_assoc_pk, AST instance to create)
                        dup_create_list = []

                        occ_assoc_to_update = []
                        for o in occ_reports_with_occ:
                            occ = getattr(o, "occurrence", None)
                            if not occ:
                                continue

                            # Check if HABITAT section exists for this sheetno
                            sheetno = o.migrated_from_id
                            is_habitat = False
                            if sheetno and sheetno in pop_section_map:
                                for _, code in pop_section_map[sheetno]:
                                    if code == "HABITAT":
                                        is_habitat = True
                                        break

                            if not is_habitat:
                                continue

                            ocr_assoc = existing_assoc.get(o.pk)
                            if not ocr_assoc:
                                continue
                            occ_assoc = existing_occ_assoc.get(occ.id)
                            if not occ_assoc:
                                continue

                            # Update OCCAssociatedSpecies fields (comment and link)
                            updated = False
                            if ocr_assoc.comment and occ_assoc.comment != ocr_assoc.comment:
                                occ_assoc.comment = ocr_assoc.comment
                                updated = True
                            if occ_assoc.copied_ocr_associated_species_id != ocr_assoc.pk:
                                occ_assoc.copied_ocr_associated_species = ocr_assoc
                                updated = True

                            if updated:
                                occ_assoc_to_update.append(occ_assoc)

                            for ast in ocr_assoc.related_species.all():
                                inst = _AST(
                                    taxonomy_id=ast.taxonomy_id,
                                    species_role_id=getattr(ast, "species_role_id", None),
                                    comments=ast.comments or "",
                                )
                                dup_create_list.append((occ_assoc.pk, inst))

                        if occ_assoc_to_update:
                            try:
                                OCCAssociatedSpecies.objects.bulk_update(
                                    occ_assoc_to_update,
                                    ["comment", "copied_ocr_associated_species"],
                                    batch_size=BATCH,
                                )
                            except Exception:
                                logger.exception(
                                    "Failed to bulk_update OCCAssociatedSpecies; falling back to individual saves"
                                )
                                for a in occ_assoc_to_update:
                                    try:
                                        a.save()
                                    except Exception:
                                        logger.exception(
                                            "Failed to update OCCAssociatedSpecies %s",
                                            getattr(a, "pk", None),
                                        )

                        if dup_create_list:
                            # Bulk create AST duplicates
                            ast_instances = [t[1] for t in dup_create_list]
                            try:
                                # Django 4.1+ bulk_create sets primary keys on instances (Postgres)
                                # We rely on this to avoid fetch-back loops.
                                _AST.objects.bulk_create(ast_instances, batch_size=BATCH)
                            except Exception:
                                logger.exception(
                                    "Failed bulk_create of AssociatedSpeciesTaxonomy duplicates; "
                                    "falling back to individual creates"
                                )
                                for inst in ast_instances:
                                    try:
                                        inst.save()
                                    except Exception:
                                        logger.exception(
                                            "Failed to create AssociatedSpeciesTaxonomy duplicate for taxonomy %s",
                                            getattr(inst, "taxonomy_id", None),
                                        )

                            # Build through rows for OCCAssociatedSpecies.related_species.through
                            through_occ = OCCAssociatedSpecies.related_species.through
                            # determine fk field names
                            occ_fk_field = None
                            tax_fk_field = None
                            for f in through_occ._meta.get_fields():
                                if (
                                    getattr(f, "remote_field", None)
                                    and getattr(f.remote_field, "model", None) == OCCAssociatedSpecies
                                ):
                                    occ_fk_field = f.name
                                if getattr(f, "remote_field", None) and getattr(f.remote_field, "model", None) == _AST:
                                    tax_fk_field = f.name

                            if occ_fk_field and tax_fk_field:
                                occ_fk_id = occ_fk_field + "_id"
                                tax_fk_id = tax_fk_field + "_id"
                                through_to_create = []

                                # Because ast_instances are references to the same objects in dup_create_list
                                # and bulk_create populates their PKs, we can iterate dup_create_list.
                                for occ_assoc_pk, inst in dup_create_list:
                                    if inst.pk:
                                        through_to_create.append(
                                            through_occ(
                                                **{
                                                    occ_fk_id: occ_assoc_pk,
                                                    tax_fk_id: inst.pk,
                                                }
                                            )
                                        )

                                if through_to_create:
                                    try:
                                        for i in range(0, len(through_to_create), BATCH):
                                            through_occ.objects.bulk_create(
                                                through_to_create[i : i + BATCH],
                                                batch_size=BATCH,
                                            )
                                    except Exception:
                                        logger.exception(
                                            "Failed to bulk_create OCC associated-species through rows; "
                                            "falling back to individual saves"
                                        )
                                        for t in through_to_create:
                                            try:
                                                t.save()
                                            except Exception:
                                                logger.exception(
                                                    "Failed to create through row for OCCAssociatedSpecies %s",
                                                    getattr(t, occ_fk_id, None),
                                                )
                except Exception:
                    logger.exception("Error duplicating AssociatedSpeciesTaxonomy for linked Occurrences")

        # Process TEC Associated Species (with comments)
        if tec_site_species_map:
            logger.info("Processing TEC Associated Species with comments...")

            # 1. Gather all taxon_name_ids
            tec_tax_ids = set()
            for details_list in tec_site_species_map.values():
                for d in details_list:
                    if d["taxon_name_id"]:
                        try:
                            tec_tax_ids.add(int(d["taxon_name_id"]))
                        except ValueError:
                            pass

            # 2. Resolve to Taxonomy PKs
            # Map taxon_name_id (Nomos ID) -> Taxonomy PK
            nomos_to_pk = {}
            if tec_tax_ids:
                nomos_to_pk = {
                    t["taxon_name_id"]: t["id"]
                    for t in Taxonomy.objects.filter(taxon_name_id__in=list(tec_tax_ids)).values("taxon_name_id", "id")
                }

            # 3. Identify TEC reports and ensure OCRAssociatedSpecies exists
            tec_mig_ids = [op["migrated_from_id"] for op in ops if op["migrated_from_id"].startswith("tec-site-")]
            tec_ocr_ids = [target_map[mid].pk for mid in tec_mig_ids if mid in target_map]

            existing_assocs = {
                a.occurrence_report_id: a
                for a in OCRAssociatedSpecies.objects.filter(occurrence_report_id__in=tec_ocr_ids)
            }

            new_assocs = []
            for mid in tec_mig_ids:
                if mid in target_map:
                    ocr_id = target_map[mid].pk
                    visit_id = mid[len("tec-site-") :]
                    if visit_id in tec_site_species_map and ocr_id not in existing_assocs:
                        new_assocs.append(OCRAssociatedSpecies(occurrence_report_id=ocr_id))

            if new_assocs:
                OCRAssociatedSpecies.objects.bulk_create(new_assocs)
                # Refresh map
                existing_assocs = {
                    a.occurrence_report_id: a
                    for a in OCRAssociatedSpecies.objects.filter(occurrence_report_id__in=tec_ocr_ids)
                }

            # 4. Create AssociatedSpeciesTaxonomy and links
            ast_batch = []
            links_batch = []  # List of OCRAssociatedSpecies instances corresponding to ast_batch

            for mid in tec_mig_ids:
                if mid in target_map:
                    ocr_id = target_map[mid].pk
                    if ocr_id in existing_assocs:
                        visit_id = mid[len("tec-site-") :]
                        specs = tec_site_species_map.get(visit_id, [])
                        ocra = existing_assocs[ocr_id]

                        for sp in specs:
                            tn_id = sp["taxon_name_id"]
                            comments = sp["comments"]
                            try:
                                tn_val = int(tn_id) if tn_id else None
                            except ValueError:
                                tn_val = None

                            tax_pk = nomos_to_pk.get(tn_val) if tn_val else None

                            if tax_pk:
                                ast = AssociatedSpeciesTaxonomy(taxonomy_id=tax_pk, comments=comments)
                                ast_batch.append(ast)
                                links_batch.append(ocra)
                            else:
                                # Log warning if taxonomy not found
                                warnings_details.append(
                                    {
                                        "migrated_from_id": mid,
                                        "column": "associated_species",
                                        "level": "warning",
                                        "message": f"Taxonomy not found for Nomos ID {tn_id}",
                                        "raw_value": tn_id,
                                        "reason": "taxonomy_lookup_failed",
                                        "row": {},
                                        "timestamp": timezone.now().isoformat(),
                                    }
                                )

            if ast_batch:
                try:
                    AssociatedSpeciesTaxonomy.objects.bulk_create(ast_batch, batch_size=BATCH)

                    # Create Through relationships
                    Through = OCRAssociatedSpecies.related_species.through
                    through_objs = []
                    for i, ast in enumerate(ast_batch):
                        ocra = links_batch[i]
                        through_objs.append(
                            Through(
                                ocrassociatedspecies_id=ocra.id,
                                associatedspeciestaxonomy_id=ast.id,
                            )
                        )

                    Through.objects.bulk_create(through_objs, batch_size=BATCH)
                    logger.info(f"Created {len(through_objs)} TEC associated species links.")
                except Exception:
                    logger.exception("Failed to bulk create TEC associated species")

        # SubmitterInformation: OneToOne - create or update submitter information
        # Note: OneToOne relationship is defined on OccurrenceReport side (submitter_information field)
        # Fetch existing submitter information (keyed by occurrence_report.pk)
        existing_submitter_info = {}
        for s in SubmitterInformation.objects.filter(occurrence_report__in=target_occs):
            # The relationship is OneToOne from OccurrenceReport to SubmitterInformation
            # Access the related OccurrenceReport through the related_name
            try:
                ocr_id = s.occurrence_report.pk
                existing_submitter_info[ocr_id] = s
            except Exception:
                # If occurrence_report is None or deleted, skip
                pass

        submitter_info_to_create = []
        submitter_info_create_map = {}  # Maps (ocr_id, mig_id) -> SubmitterInformation instance
        submitter_info_to_update = []

        for up in to_update:
            (
                inst,
                habitat_data,
                habitat_condition,
                submitter_information_data,
                location_data,
                observation_detail_data,
                geometry_data,
                plant_count_data,
                vegetation_structure_data,
                fire_history_data,
            ) = up
            sid = inst.pk
            si_data = submitter_information_data or {}

            # SubmitterInformation: update existing or schedule create
            if sid in existing_submitter_info:
                si = existing_submitter_info[sid]
                valid_si_fields = {f.name for f in SubmitterInformation._meta.fields}
                for field_name, val in si_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_si_fields:
                        apply_value_to_instance(si, field_name, val)

                # Ensure defaults are set if not already present
                if not si.organisation:
                    si.organisation = "DBCA"
                if not si.submitter_category_id:
                    try:
                        from boranga.components.users.models import SubmitterCategory

                        dbca_cat = SubmitterCategory.objects.filter(name__iexact="DBCA").first()
                        if dbca_cat:
                            si.submitter_category_id = dbca_cat.pk
                    except Exception:
                        pass

                submitter_info_to_update.append(si)
            else:
                # Create new SubmitterInformation (DON'T pass occurrence_report in create_kwargs)
                # We'll link it to OccurrenceReport after creation
                si_create = {}
                valid_si_fields = {f.name for f in SubmitterInformation._meta.fields}
                for field_name, val in si_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_si_fields:
                        si_create[field_name] = val

                # Ensure defaults for organisation and submitter_category
                if "organisation" not in si_create or si_create.get("organisation") is None:
                    si_create["organisation"] = "DBCA"

                if "submitter_category_id" not in si_create or si_create.get("submitter_category_id") is None:
                    try:
                        from boranga.components.users.models import SubmitterCategory

                        dbca_cat = SubmitterCategory.objects.filter(name__iexact="DBCA").first()
                        if dbca_cat:
                            si_create["submitter_category_id"] = dbca_cat.pk
                    except Exception:
                        pass

                if si_data:  # only create if we have data
                    si_instance = SubmitterInformation(**normalize_create_kwargs(SubmitterInformation, si_create))
                    submitter_info_to_create.append(si_instance)
                    submitter_info_create_map[(sid, None)] = si_instance  # track for linking later

        # Handle created ones (from create_meta)
        for (
            mig,
            habitat_data,
            habitat_condition,
            submitter_information_data,
            location_data,
            observation_detail_data,
            geometry_data,
            plant_count_data,
            vegetation_structure_data,
            fire_history_data,
        ) in create_meta:
            ocr = target_map.get(mig)
            if not ocr:
                continue
            si_data = submitter_information_data or {}

            # Check if submitter_information already exists (shouldn't normally happen for created)
            if ocr.pk in existing_submitter_info:
                si = existing_submitter_info[ocr.pk]
                valid_si_fields = {f.name for f in SubmitterInformation._meta.fields}
                for field_name, val in si_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_si_fields:
                        apply_value_to_instance(si, field_name, val)

                # Ensure defaults are set if not already present
                if not si.organisation:
                    si.organisation = "DBCA"
                if not si.submitter_category_id:
                    try:
                        from boranga.components.users.models import SubmitterCategory

                        dbca_cat = SubmitterCategory.objects.filter(name__iexact="DBCA").first()
                        if dbca_cat:
                            si.submitter_category_id = dbca_cat.pk
                    except Exception:
                        pass

                submitter_info_to_update.append(si)
            else:
                # Create new SubmitterInformation (DON'T pass occurrence_report in create_kwargs)
                si_create = {}
                valid_si_fields = {f.name for f in SubmitterInformation._meta.fields}
                for field_name, val in si_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_si_fields:
                        si_create[field_name] = val

                # Ensure organisation defaults to 'DBCA' if not provided
                if "organisation" not in si_create or si_create.get("organisation") is None:
                    si_create["organisation"] = "DBCA"

                # Ensure submitter_category defaults to DBCA category if not provided
                if "submitter_category_id" not in si_create or si_create.get("submitter_category_id") is None:
                    try:
                        from boranga.components.users.models import SubmitterCategory

                        dbca_cat = SubmitterCategory.objects.filter(name__iexact="DBCA").first()
                        if dbca_cat:
                            si_create["submitter_category_id"] = dbca_cat.pk
                    except Exception:
                        pass

                if si_data:  # only create if we have data
                    si_instance = SubmitterInformation(**normalize_create_kwargs(SubmitterInformation, si_create))
                    submitter_info_to_create.append(si_instance)
                    submitter_info_create_map[(ocr.pk, mig)] = si_instance  # track for linking later

        # Bulk update existing SubmitterInformation records with defaults
        if submitter_info_to_update:
            logger.info(
                "OccurrenceReportImporter: bulk-updating %d SubmitterInformation records",
                len(submitter_info_to_update),
            )

            # Determine which fields have been modified (non-None values)
            update_fields = set()
            for si in submitter_info_to_update:
                for f in SubmitterInformation._meta.fields:
                    if f.name not in ("id", "occurrence_report"):
                        val = getattr(si, f.name, None)
                        if val is not None or f.name in (
                            "organisation",
                            "submitter_category_id",
                        ):
                            update_fields.add(f.name)

            update_fields = list(update_fields)

            try:
                SubmitterInformation.objects.bulk_update(submitter_info_to_update, update_fields, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update SubmitterInformation; falling back to individual saves")
                for obj in submitter_info_to_update:
                    try:
                        obj.save(update_fields=update_fields)
                    except Exception as exc:
                        logger.exception("Failed to update SubmitterInformation %s", obj.pk)
                        # Try to find related OCR for error reporting
                        try:
                            # Reverse lookup
                            rel_ocr = OccurrenceReport.objects.filter(submitter_information=obj).first()
                            mig_id = rel_ocr.migrated_from_id if rel_ocr else "unknown"
                            ocr_pk = rel_ocr.pk if rel_ocr else "unknown"
                        except Exception:
                            mig_id = "unknown"
                            ocr_pk = "unknown"

                        errors_details.append(
                            {
                                "migrated_from_id": mig_id,
                                "column": "SubmitterInformation",
                                "level": "error",
                                "message": f"Failed to update submitter info: {exc}",
                                "raw_value": str(obj.pk),
                                "reason": "update_error",
                                "row": {"ocr_pk": ocr_pk, "si_pk": obj.pk},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if submitter_info_to_create:
            logger.info(
                "OccurrenceReportImporter: bulk-creating %d SubmitterInformation records",
                len(submitter_info_to_create),
            )
            try:
                SubmitterInformation.objects.bulk_create(submitter_info_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create SubmitterInformation; falling back to individual creates")
                for (ocr_id, mig), obj in submitter_info_create_map.items():
                    if obj.pk:
                        continue
                    try:
                        obj.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create SubmitterInformation for occurrence_report %s",
                            ocr_id,
                        )
                        errors_details.append(
                            {
                                "migrated_from_id": mig or "",
                                "column": "SubmitterInformation",
                                "level": "error",
                                "message": f"Failed to create submitter info: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"ocr_pk": ocr_id},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # After bulk_create, refresh created SubmitterInformation instances to get their IDs
        # and link them to OccurrenceReports
        if submitter_info_create_map:
            occs_to_link_si = []

            # Pre-fetch any OccurrenceReports that might be missing from target_map
            missing_ocr_ids = {
                ocr_id
                for (ocr_id, mig) in submitter_info_create_map.keys()
                if (not mig or mig not in target_map) and ocr_id
            }
            fetched_ocrs = {}
            if missing_ocr_ids:
                fetched_ocrs = OccurrenceReport.objects.in_bulk(list(missing_ocr_ids))

            for (ocr_id, mig), si_instance in submitter_info_create_map.items():
                # No need to refresh_from_db() as bulk_create populates PKs in Django 5.x+ with Postgres

                ocr = target_map.get(mig) if mig else None
                if not ocr and ocr_id:
                    ocr = fetched_ocrs.get(ocr_id)

                if ocr:
                    ocr.submitter_information_id = si_instance.pk
                    occs_to_link_si.append(ocr)

            if occs_to_link_si:
                try:
                    OccurrenceReport.objects.bulk_update(
                        occs_to_link_si, ["submitter_information_id"], batch_size=BATCH
                    )
                except Exception:
                    logger.exception("Failed to link SubmitterInformation to OccurrenceReport")
                    # Fallback to individual saves if bulk_update fails
                    for ocr in occs_to_link_si:
                        try:
                            ocr.save(update_fields=["submitter_information_id"])
                        except Exception:
                            logger.exception(
                                "Failed to link SubmitterInformation for OccurrenceReport %s",
                                ocr.pk,
                            )

        # OccurrenceReportDocument: Create from SV_PHOTO (Task 12502-12508)
        # Note: We create a text file containing the reference if no file is provided.
        doc_cat_photo = DocumentCategory.objects.filter(document_category_name="ORF Document").first()
        doc_sub_photo = DocumentSubCategory.objects.filter(document_sub_category_name="Photo").first()

        for mid in target_mig_ids:
            if mid in op_map:
                op = op_map[mid]
                photo_ref = op["merged"].get("temp_sv_photo")
                if photo_ref:
                    ocr = target_map[mid]
                    # Check if already exists to avoid duplication
                    if not OccurrenceReportDocument.objects.filter(
                        occurrence_report=ocr, description=photo_ref
                    ).exists():
                        try:
                            doc = OccurrenceReportDocument(
                                occurrence_report=ocr,
                                description=photo_ref,
                                document_category=doc_cat_photo,
                                document_sub_category=doc_sub_photo,
                                can_submitter_access=False,
                            )
                            # User request: No dummy file needed, just the description.
                            # However, if _file is mandatory at model level, this might fail saving.
                            # Assuming _file is optional or handled otherwise.
                            # If _file is mandatory, we might need to skip validation or save with empty string?
                            # Models generally require a file for FileField unless blank=True.
                            # Checking OccurrenceReportDocument definition:
                            # _file = models.FileField(...) -> blank=False by default.
                            doc.save()
                        except Exception:
                            logger.exception("Failed to create legacy photo document for %s", mid)

                # TFAUNA document description (Map/MudMap/Photo/Notes flags)
                doc_desc = op["merged"].get("temp_document_description")
                if doc_desc:
                    ocr = target_map[mid]
                    if not OccurrenceReportDocument.objects.filter(
                        occurrence_report=ocr, description=doc_desc
                    ).exists():
                        try:
                            doc = OccurrenceReportDocument(
                                occurrence_report=ocr,
                                description=doc_desc,
                                document_category=doc_cat_photo,
                                document_sub_category=doc_sub_photo,
                                can_submitter_access=False,
                            )
                            doc.save()
                        except Exception:
                            logger.exception("Failed to create TFAUNA document for %s", mid)

        # OCRObserverDetail: ensure a main observer exists for each occurrence_report
        want_obs_create = []
        existing_obs = set(
            OCRObserverDetail.objects.filter(occurrence_report__in=target_occs, main_observer=True).values_list(
                "occurrence_report_id", flat=True
            )
        )
        for mig in target_mig_ids:
            ocr = target_map.get(mig)
            if not ocr:
                continue
            if ocr.pk in existing_obs:
                # already has main observer
                continue
            # find merged data for this migrated id to populate name and role
            # lookup merged data from op_map to populate name and role
            observer_name = None
            observer_role = None
            observer_contact = None
            observer_organisation = None
            op = op_map.get(mig)
            if op:
                merged = op.get("merged") or {}
                observer_name = merged.get("OCRObserverDetail__observer_name")
                observer_role = merged.get("OCRObserverDetail__role")
                observer_contact = merged.get("OCRObserverDetail__contact")
                observer_organisation = merged.get("OCRObserverDetail__organisation")

            # create observer instance after searching ops so the variables
            # `observer_name` and `observer_role` are defined regardless of
            # whether the loop hit the break path
            ocr_observer_detail_instance = OCRObserverDetail(
                occurrence_report=ocr,
                main_observer=True,
                visible=True,
            )
            apply_value_to_instance(ocr_observer_detail_instance, "observer_name", observer_name)
            apply_value_to_instance(ocr_observer_detail_instance, "role", observer_role)
            apply_value_to_instance(ocr_observer_detail_instance, "contact", observer_contact)
            apply_value_to_instance(ocr_observer_detail_instance, "organisation", observer_organisation)

            want_obs_create.append(ocr_observer_detail_instance)

        if want_obs_create:
            try:
                OCRObserverDetail.objects.bulk_create(want_obs_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRObserverDetail; falling back to individual creates")
                for obs in want_obs_create:
                    try:
                        obs.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRObserverDetail for occurrence_report %s",
                            getattr(obs.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(obs, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRObserverDetail",
                                "level": "error",
                                "message": f"Failed to create observer detail: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRHabitatComposition: OneToOne - create or update loose_rock_percent
        # Fetch existing habitat comps
        existing_habs = {
            h.occurrence_report_id: h for h in OCRHabitatComposition.objects.filter(occurrence_report__in=target_occs)
        }
        # Fetch existing habitat conditions
        existing_conds = {
            c.occurrence_report_id: c for c in OCRHabitatCondition.objects.filter(occurrence_report__in=target_occs)
        }
        # Fetch existing identifications
        existing_idents = {
            it.occurrence_report_id: it for it in OCRIdentification.objects.filter(occurrence_report__in=target_occs)
        }
        # Fetch existing locations
        existing_locations = {
            loc.occurrence_report_id: loc for loc in OCRLocation.objects.filter(occurrence_report__in=target_occs)
        }
        # Fetch existing observation details
        existing_observations = {
            od.occurrence_report_id: od for od in OCRObservationDetail.objects.filter(occurrence_report__in=target_occs)
        }
        existing_plant_counts = {
            pc.occurrence_report_id: pc for pc in OCRPlantCount.objects.filter(occurrence_report__in=target_occs)
        }
        existing_animal_observations = {
            ao.occurrence_report_id: ao for ao in OCRAnimalObservation.objects.filter(occurrence_report__in=target_occs)
        }
        existing_vegetation_structures = {
            vs.occurrence_report_id: vs
            for vs in OCRVegetationStructure.objects.filter(occurrence_report__in=target_occs)
        }
        existing_fire_histories = {
            fh.occurrence_report_id: fh for fh in OCRFireHistory.objects.filter(occurrence_report__in=target_occs)
        }
        intensity_map = {i.name: i for i in Intensity.objects.all()}
        intensity_id_map = {i.id: i for i in Intensity.objects.all()}
        habs_to_create = []
        habs_to_update = []
        conds_to_create = []
        conds_to_update = []
        idents_to_create = []
        idents_to_update = []
        locs_to_create = []
        locs_to_update = []
        obs_to_create = []
        obs_to_update = []
        plant_counts_to_create = []
        plant_counts_to_update = []
        animal_obs_to_create = []
        animal_obs_to_update = []
        vegetation_structures_to_create = []
        vegetation_structures_to_update = []
        fire_history_to_create = []
        fire_history_to_update = []

        # Fetch ContentType once before processing geometries
        from django.contrib.contenttypes.models import ContentType

        ocr_content_type = ContentType.objects.get_for_model(OccurrenceReport)

        # Pre-fetch existing geometries for bulk lookup
        # For updates:
        update_ocr_ids = [t[0].pk for t in to_update]
        # For creates:
        create_ocr_ids = [s.pk for s in created_map.values()]

        all_ocr_ids = update_ocr_ids + create_ocr_ids

        existing_ocr_geoms = {
            g.occurrence_report_id: g
            for g in OccurrenceReportGeometry.objects.filter(occurrence_report_id__in=all_ocr_ids)
        }

        for up in to_update:
            (
                inst,
                habitat_data,
                habitat_condition,
                submitter_information_data,
                location_data,
                observation_detail_data,
                geometry_data,
                plant_count_data,
                vegetation_structure_data,
                fire_history_data,
            ) = up
            hid = inst.pk
            # identification: identification_data for updates will be looked up from `ops` by migrated_from_id
            hd = habitat_data or {}
            hc = habitat_condition or {}
            # OCRHabitatComposition: update existing or schedule create (use inst/hid)
            if hid in existing_habs:
                h = existing_habs[hid]
                valid_fields = {f.name for f in OCRHabitatComposition._meta.fields}
                for field_name, val in hd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fields:
                        apply_value_to_instance(h, field_name, val)
                habs_to_update.append(h)
            else:
                create_kwargs = {"occurrence_report": inst}
                valid_fields = {f.name for f in OCRHabitatComposition._meta.fields}
                for field_name, val in hd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fields:
                        create_kwargs[field_name] = val
                habs_to_create.append(
                    OCRHabitatComposition(**normalize_create_kwargs(OCRHabitatComposition, create_kwargs))
                )
            # OCRHabitatCondition handling for updates: check existing_conds
            if hid in existing_conds:
                c = existing_conds[hid]
                valid_c_fields = {f.name for f in OCRHabitatCondition._meta.fields}
                for field_name, val in hc.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_c_fields:
                        apply_value_to_instance(c, field_name, val)
                conds_to_update.append(c)
            else:
                cond_create = {"occurrence_report": inst}
                valid_c_fields = {f.name for f in OCRHabitatCondition._meta.fields}
                for field_name, val in hc.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_c_fields:
                        cond_create[field_name] = val
                conds_to_create.append(OCRHabitatCondition(**normalize_create_kwargs(OCRHabitatCondition, cond_create)))
            # OCRIdentification handling for updates: try to pull identification_data from op mapping created earlier
            # find corresponding op by migrated_from_id -> inst.migrated_from_id is not stored on inst;
            # instead use target_map reverse lookup
            try:
                mig_key = inst.migrated_from_id
            except Exception:
                mig_key = None
            ident_data = {}
            if mig_key:
                # find op entry for this migrated_from_id
                # constant-time lookup via op_map
                op = op_map.get(mig_key)
                if op:
                    ident_data = op.get("identification_data") or {}

            if hid in existing_idents:
                id_obj = existing_idents[hid]
                valid_i_fields = {f.name for f in OCRIdentification._meta.fields}
                for field_name, val in (ident_data or {}).items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_i_fields:
                        apply_value_to_instance(id_obj, field_name, val)
                idents_to_update.append(id_obj)
            else:
                create_kwargs = {"occurrence_report_id": hid}
                valid_i_fields = {f.name for f in OCRIdentification._meta.fields}
                for field_name, val in (ident_data or {}).items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_i_fields:
                        create_kwargs[field_name] = val
                idents_to_create.append(OCRIdentification(**normalize_create_kwargs(OCRIdentification, create_kwargs)))

            # OCRLocation handling for updates
            ld = location_data or {}
            # Fetch existing location for this occurrence_report if not already fetched
            if hid in existing_locations:
                loc_obj = existing_locations[hid]
                valid_loc_fields = {f.name for f in OCRLocation._meta.fields}
                for field_name, val in ld.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_loc_fields:
                        apply_value_to_instance(loc_obj, field_name, val)
                locs_to_update.append(loc_obj)
            else:
                create_kwargs = {"occurrence_report_id": hid}
                valid_loc_fields = {f.name for f in OCRLocation._meta.fields}
                for field_name, val in ld.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_loc_fields:
                        create_kwargs[field_name] = val
                locs_to_create.append(OCRLocation(**normalize_create_kwargs(OCRLocation, create_kwargs)))

            # OCRObservationDetail handling for updates
            od = observation_detail_data or {}
            if hid in existing_observations:
                obs_obj = existing_observations[hid]
                valid_obs_fields = {f.name for f in OCRObservationDetail._meta.fields}
                for field_name, val in od.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_obs_fields:
                        apply_value_to_instance(obs_obj, field_name, val)
                obs_to_update.append(obs_obj)
            else:
                obs_create = {"occurrence_report": inst}
                valid_obs_fields = {f.name for f in OCRObservationDetail._meta.fields}
                for field_name, val in od.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_obs_fields:
                        obs_create[field_name] = val
                if len(od) > 0:
                    obs_to_create.append(
                        OCRObservationDetail(**normalize_create_kwargs(OCRObservationDetail, obs_create))
                    )

            # OCRPlantCount handling for updates
            pcd = plant_count_data or {}
            if hid in existing_plant_counts:
                pc_obj = existing_plant_counts[hid]
                valid_pc_fields = {f.name for f in OCRPlantCount._meta.fields}
                for field_name, val in pcd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_pc_fields:
                        apply_value_to_instance(pc_obj, field_name, val)
                plant_counts_to_update.append(pc_obj)
            else:
                pc_create = {"occurrence_report": inst}
                valid_pc_fields = {f.name for f in OCRPlantCount._meta.fields}
                for field_name, val in pcd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_pc_fields:
                        pc_create[field_name] = val
                if len(pcd) > 0:
                    plant_counts_to_create.append(OCRPlantCount(**normalize_create_kwargs(OCRPlantCount, pc_create)))

            # OCRAnimalObservation handling for updates
            ao_data = {}
            if mig_key:
                op = op_map.get(mig_key)
                if op:
                    ao_data = op.get("animal_observation_data") or {}
            if hid in existing_animal_observations:
                ao_obj = existing_animal_observations[hid]
                valid_ao_fields = {f.name for f in OCRAnimalObservation._meta.fields}
                for field_name, val in ao_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_ao_fields:
                        apply_value_to_instance(ao_obj, field_name, val)
                animal_obs_to_update.append(ao_obj)
            else:
                ao_create = {"occurrence_report": inst}
                valid_ao_fields = {f.name for f in OCRAnimalObservation._meta.fields}
                for field_name, val in ao_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_ao_fields:
                        ao_create[field_name] = val
                if len(ao_data) > 0:
                    animal_obs_to_create.append(
                        OCRAnimalObservation(**normalize_create_kwargs(OCRAnimalObservation, ao_create))
                    )

            # OCRVegetationStructure handling for updates
            vsd = vegetation_structure_data or {}
            if hid in existing_vegetation_structures:
                vs_obj = existing_vegetation_structures[hid]
                valid_vs_fields = {f.name for f in OCRVegetationStructure._meta.fields}
                for field_name, val in vsd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_vs_fields:
                        apply_value_to_instance(vs_obj, field_name, val)
                vegetation_structures_to_update.append(vs_obj)
            else:
                vs_create = {"occurrence_report": inst}
                valid_vs_fields = {f.name for f in OCRVegetationStructure._meta.fields}
                for field_name, val in vsd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_vs_fields:
                        vs_create[field_name] = val
                if len(vsd) > 0:
                    vegetation_structures_to_create.append(
                        OCRVegetationStructure(**normalize_create_kwargs(OCRVegetationStructure, vs_create))
                    )

            # OCRFireHistory handling for updates
            fhd = fire_history_data or {}

            # Transform fields
            comment = fhd.get("comment")
            intensity_name = fhd.get("intensity")

            intensity_obj = None
            if intensity_name:
                intensity_obj = intensity_map.get(intensity_name)

            fh_fields = {}
            if comment:
                fh_fields["comment"] = comment
            if intensity_obj:
                fh_fields["intensity"] = intensity_obj

            if hid in existing_fire_histories:
                fh_obj = existing_fire_histories[hid]
                valid_fh_fields = {f.name for f in OCRFireHistory._meta.fields}
                for field_name, val in fh_fields.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fh_fields:
                        apply_value_to_instance(fh_obj, field_name, val)
                fire_history_to_update.append(fh_obj)
            else:
                fh_create = {"occurrence_report": inst}
                valid_fh_fields = {f.name for f in OCRFireHistory._meta.fields}
                for field_name, val in fh_fields.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fh_fields:
                        fh_create[field_name] = val
                if len(fh_fields) > 0:
                    fire_history_to_create.append(OCRFireHistory(**normalize_create_kwargs(OCRFireHistory, fh_create)))

            # OccurrenceReportGeometry handling for updates
            gd = geometry_data or {}
            if gd.get("geometry"):
                existing_geom = existing_ocr_geoms.get(inst.pk)

                if existing_geom:
                    # Update existing geometry
                    valid_geom_fields = {f.name for f in OccurrenceReportGeometry._meta.fields}
                    for field_name, val in gd.items():
                        if field_name == "occurrence_report":
                            continue
                        if val is not None and field_name in valid_geom_fields:
                            apply_value_to_instance(existing_geom, field_name, val)
                    try:
                        existing_geom.save()
                    except Exception:
                        logger.exception(
                            "Failed to update OccurrenceReportGeometry for occurrence_report %s",
                            inst.pk,
                        )
                else:
                    # Create new geometry
                    geom_create_kwargs = {
                        "occurrence_report_id": inst.pk,
                        "content_type": ocr_content_type,
                        "object_id": inst.pk,
                    }
                    valid_geom_fields = {f.name for f in OccurrenceReportGeometry._meta.fields}
                    for field_name, val in gd.items():
                        if field_name == "occurrence_report":
                            continue
                        if val is not None and field_name in valid_geom_fields:
                            geom_create_kwargs[field_name] = val

                    try:
                        buffered_geom = gd.get("geometry")
                        if buffered_geom and hasattr(buffered_geom, "centroid"):
                            original_point = buffered_geom.centroid
                            if original_point:
                                geom_create_kwargs["original_geometry_ewkb"] = original_point.ewkb
                    except Exception:
                        pass
                        # logger.debug("Could not extract original point geometry")

                    try:
                        new_geom = OccurrenceReportGeometry.objects.create(
                            **normalize_create_kwargs(OccurrenceReportGeometry, geom_create_kwargs)
                        )
                        existing_ocr_geoms[inst.pk] = new_geom
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OccurrenceReportGeometry for occurrence_report %s",
                            inst.pk,
                        )
                        errors_details.append(
                            {
                                "migrated_from_id": inst.migrated_from_id,
                                "column": "geometry",
                                "level": "error",
                                "message": f"Failed to create geometry: {exc}",
                                "raw_value": str(gd.get("geometry", "")),
                                "reason": "geometry_creation_error",
                                "row": {"pk": inst.pk},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

                # If there is a related Occurrence, logic for copying geometry has been moved
                # to the final population phase based on DRF_POP_SECTION_MAP (SECT_CODE='LOCATION')

        # Handle created ones
        logger.debug(f"Processing create_meta: len={len(create_meta)}, created_map len={len(created_map)}")
        # logger.info(f"created_map keys: {list(created_map.keys())}")

        cm_processed = 0
        for (
            mig,
            habitat_data,
            habitat_condition,
            submitter_information_data,
            location_data,
            observation_detail_data,
            geometry_data,
            plant_count_data,
            vegetation_structure_data,
            fire_history_data,
        ) in create_meta:
            cm_processed += 1
            if cm_processed % 500 == 0:
                logger.info(
                    "OccurrenceReportImporter: processing create_meta item %d/%d (mig=%s)",
                    cm_processed,
                    len(create_meta),
                    mig,
                )

            ocr = created_map.get(mig)
            if not ocr:
                logger.debug(f"Skipping {mig}: not in created_map")
                continue
            # logger.debug(f"Continuing with mig={mig}, ocr.pk={ocr.pk}")
            hd = habitat_data or {}
            hc = habitat_condition or {}
            # also pull identification_data from create_meta mapping (create_meta entries are tuples of
            # (migrated_from_id, habitat_data, habitat_condition, identification_data) )
            # but create_meta was appended as (migrated_from_id, habitat_data, habitat_condition) earlier;
            # we need to find the op to get identification_data
            ident_data = {}
            ident_data = op_map.get(mig, {}).get("identification_data") or {}
            ld = location_data or {}
            if ocr.pk in existing_habs:
                h = existing_habs[ocr.pk]
                valid_fields = {f.name for f in OCRHabitatComposition._meta.fields}
                for field_name, val in hd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fields:
                        apply_value_to_instance(h, field_name, val)
                habs_to_update.append(h)
            else:
                create_kwargs = {"occurrence_report": ocr}
                valid_fields = {f.name for f in OCRHabitatComposition._meta.fields}
                for field_name, val in hd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fields:
                        create_kwargs[field_name] = val
                habs_to_create.append(
                    OCRHabitatComposition(**normalize_create_kwargs(OCRHabitatComposition, create_kwargs))
                )
            # OCRHabitatCondition create/update for newly created ocr
            if ocr.pk in existing_conds:
                c = existing_conds[ocr.pk]
                valid_c_fields = {f.name for f in OCRHabitatCondition._meta.fields}
                for field_name, val in hc.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_c_fields:
                        apply_value_to_instance(c, field_name, val)
                conds_to_update.append(c)
            else:
                cond_create = {"occurrence_report": ocr}
                valid_c_fields = {f.name for f in OCRHabitatCondition._meta.fields}
                for field_name, val in hc.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_c_fields:
                        cond_create[field_name] = val
                conds_to_create.append(OCRHabitatCondition(**normalize_create_kwargs(OCRHabitatCondition, cond_create)))
            # identification create for newly created ocr
            if ocr.pk in existing_idents:
                id_obj = existing_idents[ocr.pk]
                valid_i_fields = {f.name for f in OCRIdentification._meta.fields}
                for field_name, val in ident_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_i_fields:
                        apply_value_to_instance(id_obj, field_name, val)
                idents_to_update.append(id_obj)
            else:
                create_kwargs = {"occurrence_report": ocr}
                valid_i_fields = {f.name for f in OCRIdentification._meta.fields}
                for field_name, val in ident_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_i_fields:
                        create_kwargs[field_name] = val
                idents_to_create.append(OCRIdentification(**normalize_create_kwargs(OCRIdentification, create_kwargs)))

            # OCRObservationDetail: OneToOne - create or update survey fields
            od = observation_detail_data or {}
            if ocr.pk in existing_observations:
                obs_obj = existing_observations[ocr.pk]
                valid_obs_fields = {f.name for f in OCRObservationDetail._meta.fields}
                for field_name, val in od.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_obs_fields:
                        apply_value_to_instance(obs_obj, field_name, val)
                obs_to_update.append(obs_obj)
            else:
                obs_create = {"occurrence_report": ocr}
                valid_obs_fields = {f.name for f in OCRObservationDetail._meta.fields}
                for field_name, val in od.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_obs_fields:
                        obs_create[field_name] = val
                # Only create OCRObservationDetail if we have data
                if len(od) > 0:
                    obs_to_create.append(
                        OCRObservationDetail(**normalize_create_kwargs(OCRObservationDetail, obs_create))
                    )

            # OCRPlantCount handling for newly created ocr
            pcd = plant_count_data or {}
            if ocr.pk in existing_plant_counts:
                pc_obj = existing_plant_counts[ocr.pk]
                valid_pc_fields = {f.name for f in OCRPlantCount._meta.fields}
                for field_name, val in pcd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_pc_fields:
                        apply_value_to_instance(pc_obj, field_name, val)
                plant_counts_to_update.append(pc_obj)
            else:
                pc_create = {"occurrence_report": ocr}
                valid_pc_fields = {f.name for f in OCRPlantCount._meta.fields}
                for field_name, val in pcd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_pc_fields:
                        pc_create[field_name] = val
                if len(pcd) > 0:
                    plant_counts_to_create.append(OCRPlantCount(**normalize_create_kwargs(OCRPlantCount, pc_create)))

            # OCRAnimalObservation handling for newly created ocr
            ao_data = op_map.get(mig, {}).get("animal_observation_data") or {}
            if ocr.pk in existing_animal_observations:
                ao_obj = existing_animal_observations[ocr.pk]
                valid_ao_fields = {f.name for f in OCRAnimalObservation._meta.fields}
                for field_name, val in ao_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_ao_fields:
                        apply_value_to_instance(ao_obj, field_name, val)
                animal_obs_to_update.append(ao_obj)
            else:
                ao_create = {"occurrence_report": ocr}
                valid_ao_fields = {f.name for f in OCRAnimalObservation._meta.fields}
                for field_name, val in ao_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_ao_fields:
                        ao_create[field_name] = val
                if len(ao_data) > 0:
                    animal_obs_to_create.append(
                        OCRAnimalObservation(**normalize_create_kwargs(OCRAnimalObservation, ao_create))
                    )

            # OCRVegetationStructure handling for newly created ocr
            vsd = vegetation_structure_data or {}
            # Note: existing_vegetation_structures is keyed by occurrence_report_id
            # but for newly created OCRs, we might not have them in existing_vegetation_structures
            # unless we re-fetched them (which we didn't).
            # However, since these are newly created OCRs, they shouldn't have existing VS unless
            # something weird happened.
            # But let's check anyway if we want to be safe, or just assume create.
            # Actually, we are iterating over create_meta, so these are definitely new OCRs.
            # So we just create.

            vs_create = {"occurrence_report": ocr}
            valid_vs_fields = {f.name for f in OCRVegetationStructure._meta.fields}
            for field_name, val in vsd.items():
                if field_name == "occurrence_report":
                    continue
                if val is not None and field_name in valid_vs_fields:
                    vs_create[field_name] = val
            if len(vsd) > 0:
                vegetation_structures_to_create.append(
                    OCRVegetationStructure(**normalize_create_kwargs(OCRVegetationStructure, vs_create))
                )

            # OCRFireHistory handling for newly created ocr
            fhd = fire_history_data or {}

            # Transform fields
            comment = fhd.get("comment")
            intensity_val = fhd.get("intensity")

            intensity_obj = None
            if intensity_val:
                if isinstance(intensity_val, int):
                    intensity_obj = intensity_id_map.get(intensity_val)
                else:
                    intensity_obj = intensity_map.get(intensity_val)

            fh_fields = {}
            if comment:
                fh_fields["comment"] = comment
            if intensity_obj:
                fh_fields["intensity"] = intensity_obj

            if ocr.pk in existing_fire_histories:
                fh_obj = existing_fire_histories[ocr.pk]
                valid_fh_fields = {f.name for f in OCRFireHistory._meta.fields}
                for field_name, val in fh_fields.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fh_fields:
                        apply_value_to_instance(fh_obj, field_name, val)
                fire_history_to_update.append(fh_obj)
            else:
                fh_create = {"occurrence_report": ocr}
                valid_fh_fields = {f.name for f in OCRFireHistory._meta.fields}
                for field_name, val in fh_fields.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fh_fields:
                        fh_create[field_name] = val
                if len(fh_fields) > 0:
                    fire_history_to_create.append(OCRFireHistory(**normalize_create_kwargs(OCRFireHistory, fh_create)))

            # OCRLocation create/update for newly created ocr
            if ocr.pk in existing_locations:
                loc_obj = existing_locations[ocr.pk]
                valid_loc_fields = {f.name for f in OCRLocation._meta.fields}
                for field_name, val in ld.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_loc_fields:
                        apply_value_to_instance(loc_obj, field_name, val)
                locs_to_update.append(loc_obj)
            else:
                create_kwargs = {"occurrence_report": ocr}
                valid_loc_fields = {f.name for f in OCRLocation._meta.fields}
                for field_name, val in ld.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_loc_fields:
                        create_kwargs[field_name] = val
                locs_to_create.append(OCRLocation(**normalize_create_kwargs(OCRLocation, create_kwargs)))

            # OccurrenceReportGeometry: OneToOne - create geometry with locked=True and content_type set
            gd = geometry_data or {}

            # Only create geometry if we have at least a geometry field
            if gd.get("geometry"):
                # logger.debug(
                #    f"Creating geometry for OCR {ocr.pk}: {type(gd.get('geometry'))}"
                # )
                existing_geom = existing_ocr_geoms.get(ocr.pk)

                if existing_geom:
                    # Update existing geometry
                    valid_geom_fields = {f.name for f in OccurrenceReportGeometry._meta.fields}
                    for field_name, val in gd.items():
                        if field_name == "occurrence_report":
                            continue
                        if val is not None and field_name in valid_geom_fields:
                            apply_value_to_instance(existing_geom, field_name, val)
                    try:
                        existing_geom.save()
                    except Exception:
                        logger.exception(
                            "Failed to update OccurrenceReportGeometry for occurrence_report %s",
                            ocr.pk,
                        )
                else:
                    # Create new geometry
                    geom_create_kwargs = {
                        "occurrence_report_id": ocr.pk,
                        "content_type": ocr_content_type,
                        "object_id": ocr.pk,
                    }
                    # Add geometry fields from gd
                    valid_geom_fields = {f.name for f in OccurrenceReportGeometry._meta.fields}
                    for field_name, val in gd.items():
                        if field_name == "occurrence_report":
                            continue
                        if val is not None and field_name in valid_geom_fields:
                            geom_create_kwargs[field_name] = val

                    # Store the original point geometry (before buffering) in EWKB format
                    # The buffered polygon is in gd['geometry'], but we also want to preserve
                    # the original point for reference
                    try:
                        buffered_geom = gd.get("geometry")
                        if buffered_geom and hasattr(buffered_geom, "centroid"):
                            # Extract the centroid of the buffered polygon as the original point
                            original_point = buffered_geom.centroid
                            if original_point:
                                geom_create_kwargs["original_geometry_ewkb"] = original_point.ewkb
                    except Exception:
                        pass
                        # logger.debug("Could not extract original point geometry")

                    try:
                        new_geom = OccurrenceReportGeometry.objects.create(
                            **normalize_create_kwargs(OccurrenceReportGeometry, geom_create_kwargs)
                        )
                        existing_ocr_geoms[ocr.pk] = new_geom
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OccurrenceReportGeometry for occurrence_report %s",
                            ocr.pk,
                        )
                        errors_details.append(
                            {
                                "migrated_from_id": mig,
                                "column": "geometry",
                                "level": "error",
                                "message": f"Failed to create geometry: {exc}",
                                "raw_value": str(gd.get("geometry", "")),
                                "reason": "geometry_creation_error",
                                "row": {"pk": ocr.pk},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

            # Geometry copying to Occurrence is handled in the final population phase

        if habs_to_create:
            try:
                OCRHabitatComposition.objects.bulk_create(habs_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRHabitatComposition; falling back to individual creates")
                for h in habs_to_create:
                    try:
                        h.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRHabitatComposition for occurrence_report %s",
                            getattr(h.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(h, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRHabitatComposition",
                                "level": "error",
                                "message": f"Failed to create habitat composition: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if habs_to_update:
            try:
                OCRHabitatComposition.objects.bulk_update(habs_to_update, ["loose_rock_percent"], batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OCRHabitatComposition; falling back to individual saves")
                for h in habs_to_update:
                    try:
                        h.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRHabitatComposition %s",
                            getattr(h, "pk", None),
                        )
                        ocr_ref = getattr(h, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRHabitatComposition",
                                "level": "error",
                                "message": f"Failed to update habitat composition: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(h, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRHabitatCondition: OneToOne - create or update percentage flags
        if conds_to_create:
            try:
                OCRHabitatCondition.objects.bulk_create(conds_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRHabitatCondition; falling back to individual creates")
                for c in conds_to_create:
                    try:
                        c.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRHabitatCondition for occurrence_report %s",
                            getattr(c.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(c, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRHabitatCondition",
                                "level": "error",
                                "message": f"Failed to create habitat condition: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if conds_to_update:
            try:
                # determine fields to update from condition instances
                cond_fields = set()
                for inst in conds_to_update:
                    cond_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                # ensure occurrence_report_id or id not included
                cond_fields = {f for f in cond_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if cond_fields:
                    OCRHabitatCondition.objects.bulk_update(conds_to_update, list(cond_fields), batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OCRHabitatCondition; falling back to individual saves")
                for c in conds_to_update:
                    try:
                        c.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRHabitatCondition %s",
                            getattr(c, "pk", None),
                        )
                        ocr_ref = getattr(c, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRHabitatCondition",
                                "level": "error",
                                "message": f"Failed to update habitat condition: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(c, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRIdentification: OneToOne - create or update identification records
        if idents_to_create:
            try:
                OCRIdentification.objects.bulk_create(idents_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRIdentification; falling back to individual creates")
                for i in idents_to_create:
                    try:
                        i.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRIdentification for occurrence_report %s",
                            getattr(i.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(i, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRIdentification",
                                "level": "error",
                                "message": f"Failed to create identification: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if idents_to_update:
            try:
                ident_fields = set()
                for inst in idents_to_update:
                    ident_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                # exclude id or FK reference
                ident_fields = {f for f in ident_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if ident_fields:
                    OCRIdentification.objects.bulk_update(idents_to_update, list(ident_fields), batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OCRIdentification; falling back to individual saves")
                for i in idents_to_update:
                    try:
                        i.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRIdentification %s",
                            getattr(i, "pk", None),
                        )
                        ocr_ref = getattr(i, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRIdentification",
                                "level": "error",
                                "message": f"Failed to update identification: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(i, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRLocation: OneToOne - create or update location records
        if locs_to_create:
            try:
                OCRLocation.objects.bulk_create(locs_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRLocation; falling back to individual creates")
                for loc in locs_to_create:
                    try:
                        loc.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRLocation for occurrence_report %s",
                            getattr(loc.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(loc, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRLocation",
                                "level": "error",
                                "message": f"Failed to create location: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if locs_to_update:
            try:
                loc_fields = set()
                for inst in locs_to_update:
                    loc_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                # exclude id or FK reference
                loc_fields = {f for f in loc_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if loc_fields:
                    OCRLocation.objects.bulk_update(locs_to_update, list(loc_fields), batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OCRLocation; falling back to individual saves")
                for loc in locs_to_update:
                    try:
                        loc.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRLocation %s",
                            getattr(loc, "pk", None),
                        )
                        ocr_ref = getattr(loc, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRLocation",
                                "level": "error",
                                "message": f"Failed to update location: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(loc, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRObservationDetail: OneToOne - create or update observation detail records
        if obs_to_create:
            try:
                OCRObservationDetail.objects.bulk_create(obs_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRObservationDetail; falling back to individual creates")
                for obs in obs_to_create:
                    try:
                        obs.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRObservationDetail for occurrence_report %s",
                            getattr(obs.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(obs, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRObservationDetail",
                                "level": "error",
                                "message": f"Failed to create observation detail: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if obs_to_update:
            try:
                obs_fields = set()
                for inst in obs_to_update:
                    obs_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                # exclude id or FK reference
                obs_fields = {f for f in obs_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if obs_fields:
                    OCRObservationDetail.objects.bulk_update(obs_to_update, list(obs_fields), batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OCRObservationDetail; falling back to individual saves")
                for obs in obs_to_update:
                    try:
                        obs.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRObservationDetail %s",
                            getattr(obs, "pk", None),
                        )
                        ocr_ref = getattr(obs, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRObservationDetail",
                                "level": "error",
                                "message": f"Failed to update observation detail: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(obs, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRPlantCount: OneToOne - create or update plant count records
        if plant_counts_to_create:
            try:
                OCRPlantCount.objects.bulk_create(plant_counts_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRPlantCount; falling back to individual creates")
                for pc in plant_counts_to_create:
                    try:
                        pc.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRPlantCount for occurrence_report %s",
                            getattr(pc.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(pc, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRPlantCount",
                                "level": "error",
                                "message": f"Failed to create plant count: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if plant_counts_to_update:
            try:
                pc_fields = set()
                for inst in plant_counts_to_update:
                    pc_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                # exclude id or FK reference
                pc_fields = {f for f in pc_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if pc_fields:
                    OCRPlantCount.objects.bulk_update(plant_counts_to_update, list(pc_fields), batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OCRPlantCount; falling back to individual saves")
                for pc in plant_counts_to_update:
                    try:
                        pc.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRPlantCount %s",
                            getattr(pc, "pk", None),
                        )
                        ocr_ref = getattr(pc, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRPlantCount",
                                "level": "error",
                                "message": f"Failed to update plant count: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(pc, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRAnimalObservation: OneToOne - create or update animal observation records
        if animal_obs_to_create:
            try:
                OCRAnimalObservation.objects.bulk_create(animal_obs_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRAnimalObservation; falling back to individual creates")
                for ao in animal_obs_to_create:
                    try:
                        ao.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRAnimalObservation for occurrence_report %s",
                            getattr(ao.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(ao, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRAnimalObservation",
                                "level": "error",
                                "message": f"Failed to create animal observation: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if animal_obs_to_update:
            try:
                ao_fields = set()
                for inst in animal_obs_to_update:
                    ao_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                ao_fields = {f for f in ao_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if ao_fields:
                    OCRAnimalObservation.objects.bulk_update(animal_obs_to_update, list(ao_fields), batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_update OCRAnimalObservation; falling back to individual saves")
                for ao in animal_obs_to_update:
                    try:
                        ao.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRAnimalObservation %s",
                            getattr(ao, "pk", None),
                        )
                        ocr_ref = getattr(ao, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRAnimalObservation",
                                "level": "error",
                                "message": f"Failed to update animal observation: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(ao, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRVegetationStructure: OneToOne - create or update vegetation structure records
        if vegetation_structures_to_create:
            try:
                OCRVegetationStructure.objects.bulk_create(vegetation_structures_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRVegetationStructure; falling back to individual creates")
                for vs in vegetation_structures_to_create:
                    try:
                        vs.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRVegetationStructure for occurrence_report %s",
                            getattr(vs.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(vs, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRVegetationStructure",
                                "level": "error",
                                "message": f"Failed to create vegetation structure: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if vegetation_structures_to_update:
            try:
                vs_fields = set()
                for inst in vegetation_structures_to_update:
                    vs_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                # exclude id or FK reference
                vs_fields = {f for f in vs_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if vs_fields:
                    OCRVegetationStructure.objects.bulk_update(
                        vegetation_structures_to_update,
                        list(vs_fields),
                        batch_size=BATCH,
                    )
            except Exception:
                logger.exception("Failed to bulk_update OCRVegetationStructure; falling back to individual saves")
                for vs in vegetation_structures_to_update:
                    try:
                        vs.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRVegetationStructure %s",
                            getattr(vs, "pk", None),
                        )
                        ocr_ref = getattr(vs, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRVegetationStructure",
                                "level": "error",
                                "message": f"Failed to update vegetation structure: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(vs, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # OCRFireHistory: OneToOne - create or update fire history records
        if fire_history_to_create:
            try:
                OCRFireHistory.objects.bulk_create(fire_history_to_create, batch_size=BATCH)
            except Exception:
                logger.exception("Failed to bulk_create OCRFireHistory; falling back to individual creates")
                for fh in fire_history_to_create:
                    try:
                        fh.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OCRFireHistory for occurrence_report %s",
                            getattr(fh.occurrence_report, "pk", None),
                        )
                        ocr_ref = getattr(fh, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRFireHistory",
                                "level": "error",
                                "message": f"Failed to create fire history: {exc}",
                                "raw_value": "",
                                "reason": "create_error",
                                "row": {"pk": getattr(ocr_ref, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        if fire_history_to_update:
            try:
                fh_fields = set()
                for inst in fire_history_to_update:
                    fh_fields.update([f.name for f in inst._meta.fields if getattr(inst, f.name, None) is not None])
                # exclude id or FK reference
                fh_fields = {f for f in fh_fields if f not in ("id", "occurrence_report", "occurrence_report_id")}
                if fh_fields:
                    OCRFireHistory.objects.bulk_update(
                        fire_history_to_update,
                        list(fh_fields),
                        batch_size=BATCH,
                    )
            except Exception:
                logger.exception("Failed to bulk_update OCRFireHistory; falling back to individual saves")
                for fh in fire_history_to_update:
                    try:
                        fh.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to save OCRFireHistory %s",
                            getattr(fh, "pk", None),
                        )
                        ocr_ref = getattr(fh, "occurrence_report", None)
                        errors_details.append(
                            {
                                "migrated_from_id": getattr(ocr_ref, "migrated_from_id", ""),
                                "column": "OCRFireHistory",
                                "level": "error",
                                "message": f"Failed to update fire history: {exc}",
                                "raw_value": "",
                                "reason": "update_error",
                                "row": {"pk": getattr(fh, "pk", "")},
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                        errors += 1

        # Update stats counts for created/updated based on performed ops
        created += len(created_map)
        updated += len(to_update)

        # OccurrenceReportUserAction: TFAUNA-only — record ChDate/ChName as an action log
        user_actions_to_create = []
        for mid in target_mig_ids:
            if not mid.startswith("tfauna-"):
                continue
            op = op_map.get(mid)
            if not op:
                continue
            merged = op.get("merged") or {}
            ch_date = merged.get("ChDate")
            ch_name = merged.get("ChName")
            # Only create if we have a ChDate (the meaningful trigger)
            if not ch_date:
                continue
            ocr = target_map.get(mid)
            if not ocr:
                continue
            # Skip if action already exists for this OCR
            if OccurrenceReportUserAction.objects.filter(occurrence_report=ocr).exists():
                continue
            # Resolve user: use submitter id as fallback if ChName lookup fails
            who_id = None
            if ch_name:
                # The ChName may have been resolved to an email user id via approved_by pipeline
                # Try to resolve via the same lookup
                try:
                    from boranga.components.main.models import LegacyUsernameEmailuserMapping

                    mapping = LegacyUsernameEmailuserMapping.objects.filter(
                        legacy_system="TFAUNA", legacy_username=ch_name
                    ).first()
                    if mapping:
                        who_id = mapping.emailuser_id
                except Exception:
                    pass
            if not who_id:
                # Fallback: use the submitter from the OccurrenceReport
                who_id = ocr.submitter
            if not who_id:
                # Last resort: use a dummy value (0)
                who_id = 0

            # Parse ChDate
            when_dt = None
            try:
                from boranga.components.data_migration import utils as dm_utils

                when_dt = dm_utils.parse_date_iso(ch_date)
                # Ensure timezone-aware (assume Perth if naive)
                if when_dt and when_dt.tzinfo is None:
                    import zoneinfo

                    when_dt = when_dt.replace(tzinfo=zoneinfo.ZoneInfo("Australia/Perth"))
            except Exception:
                pass

            action_text = f"Legacy record checked by {ch_name or 'unknown'}"
            ua = OccurrenceReportUserAction(
                occurrence_report=ocr,
                who=who_id,
                what=action_text,
            )
            if when_dt:
                ua.when = when_dt
            user_actions_to_create.append(ua)

        if user_actions_to_create:
            try:
                OccurrenceReportUserAction.objects.bulk_create(user_actions_to_create, batch_size=BATCH)
                logger.info(
                    "Created %d OccurrenceReportUserAction records (TFAUNA)",
                    len(user_actions_to_create),
                )
            except Exception:
                logger.exception("Failed to bulk_create OccurrenceReportUserAction; falling back to individual creates")
                for ua in user_actions_to_create:
                    try:
                        ua.save()
                    except Exception as exc:
                        logger.exception(
                            "Failed to create OccurrenceReportUserAction for OCR %s: %s",
                            getattr(ua.occurrence_report, "pk", None),
                            exc,
                        )

        # ---------------------------------------------------------------------
        # Populate Occurrence 1-to-1 objects by cloning from OccurrenceReport
        # based on DRF_POP_SECTION_MAP.csv
        # ---------------------------------------------------------------------
        if pop_section_map:
            logger.info("Starting population of Occurrence 1-to-1 objects from OccurrenceReports...")
            # We iterate over all processed OccurrenceReports (both created and updated)
            # For each OCR, we check if its SHEETNO (migrated_from_id) is in the map.
            # If so, we clone the relevant sections to the parent Occurrence.

            # Combine created and updated OCRs into a single list for processing
            all_processed_ocrs = []
            if created_map:
                all_processed_ocrs.extend(created_map.values())
            if to_update:
                all_processed_ocrs.extend([t[0] for t in to_update])

            # Map SECT_CODE to list of (OCR_Model, OCC_Model, copied_ocr_field_name)
            section_config = {
                "LOCATION": [
                    (OCRLocation, OCCLocation, "copied_ocr_location"),
                    (
                        OccurrenceReportGeometry,
                        OccurrenceGeometry,
                        "copied_ocr_geometry",
                    ),
                ],
                "PLNT_CNT": [
                    (OCRPlantCount, OCCPlantCount, "copied_ocr_plant_count"),
                    (
                        OCRObservationDetail,
                        OCCObservationDetail,
                        "copied_ocr_observation_detail",
                    ),
                ],
                "HABITAT": [
                    (
                        OCRHabitatCondition,
                        OCCHabitatCondition,
                        "copied_ocr_habitat_condition",
                    ),
                    (
                        OCRHabitatComposition,
                        OCCHabitatComposition,
                        "copied_ocr_habitat_composition",
                    ),
                    (
                        OCRVegetationStructure,
                        OCCVegetationStructure,
                        "copied_ocr_vegetation_structure",
                    ),
                    (OCRFireHistory, OCCFireHistory, "copied_ocr_fire_history"),
                ],
                "VOUCHER": [
                    (OCRIdentification, OCCIdentification, "copied_ocr_identification"),
                ],
            }

            # Use bulk pre-fetching to avoid N+1 queries in the loop
            ocr_ids = [ocr.pk for ocr in all_processed_ocrs]
            occ_ids = [ocr.occurrence_id for ocr in all_processed_ocrs if ocr.occurrence_id]

            source_lookup = {}
            target_lookup = {}

            all_configs = []
            for cfg_list in section_config.values():
                all_configs.extend(cfg_list)

            # Pre-fetch source objects (OCR side)
            unique_ocr_models = {c[0] for c in all_configs}
            for model_class in unique_ocr_models:
                # We need {ocr_id: obj}.
                qs = model_class.objects.filter(occurrence_report_id__in=ocr_ids)
                objs = list(qs)
                # Populate dict, only set if not present (to keep first found, mimicking .first())
                lookup = {}
                for obj in objs:
                    if obj.occurrence_report_id not in lookup:
                        lookup[obj.occurrence_report_id] = obj
                source_lookup[model_class] = lookup

            # Pre-fetch target objects (OCC side)
            unique_occ_models = {c[1] for c in all_configs}
            for model_class in unique_occ_models:
                qs = model_class.objects.filter(occurrence_id__in=occ_ids)
                objs = list(qs)
                lookup = {}
                for obj in objs:
                    if obj.occurrence_id not in lookup:
                        lookup[obj.occurrence_id] = obj
                target_lookup[model_class] = lookup

            cloned_count = 0
            pop_processed = 0
            total_ocrs = len(all_processed_ocrs)
            for ocr in all_processed_ocrs:
                pop_processed += 1
                if pop_processed % 500 == 0:
                    logger.info(f"Population progress: {pop_processed}/{total_ocrs} occurrence reports processed...")

                sheetno = ocr.migrated_from_id
                if not sheetno:
                    continue

                entries = pop_section_map.get(sheetno)
                if not entries and "-" in sheetno:
                    # Try stripping prefix (e.g. "tpfl-12345" -> "12345")
                    entries = pop_section_map.get(sheetno.split("-", 1)[1])

                if not entries:
                    continue

                # Each entry is (POP_ID, SECT_CODE)
                for pop_id, sect_code in entries:
                    if sect_code not in section_config:
                        # Some codes might not be implemented or relevant
                        continue

                    configs = section_config[sect_code]

                    for ocr_model, occ_model, copied_field in configs:
                        # 1. Find the source OCR child object from lookup
                        source_obj = source_lookup.get(ocr_model, {}).get(ocr.pk)

                        if not source_obj:
                            continue

                        # 2. Find or create the target OCC child object
                        occurrence = ocr.occurrence
                        if not occurrence:
                            logger.warning(
                                f"OCR {ocr.pk} (SHEETNO={sheetno}) has no parent Occurrence. "
                                f"Skipping clone for {sect_code}."
                            )
                            continue

                        # Check if target already exists on the occurrence (from lookup)
                        target_obj = target_lookup.get(occ_model, {}).get(occurrence.pk)

                        if not target_obj:
                            # Create new instance
                            target_obj = occ_model(occurrence=occurrence)
                            # Update lookup slightly so if another OCR maps to same OCC, we reuse this obj
                            if occ_model not in target_lookup:
                                target_lookup[occ_model] = {}
                            target_lookup[occ_model][occurrence.pk] = target_obj

                        # Set content_type/object_id if model supports them (e.g. GeometryBase)
                        # We must do this explicitly and exclude them from common_fields to prevent
                        # finding/copying the source's content_type (which would be OccurrenceReport).
                        if hasattr(target_obj, "content_type_id") and hasattr(target_obj, "object_id"):
                            from django.contrib.contenttypes.models import ContentType

                            target_obj.content_type = ContentType.objects.get_for_model(occurrence)
                            target_obj.object_id = occurrence.pk

                        # 3. Clone data
                        try:
                            # Copy fields that exist in both models
                            source_fields = {f.name for f in source_obj._meta.fields}
                            target_fields = {f.name for f in target_obj._meta.fields}
                            common_fields = source_fields.intersection(target_fields)

                            for field_name in common_fields:
                                if field_name in [
                                    "id",
                                    "occurrence_report",
                                    "occurrence",
                                    "migrated_from_id",
                                    "content_type",
                                    "object_id",
                                ]:
                                    continue

                                val = getattr(source_obj, field_name)
                                # Default missing color/stroke for OccurrenceGeometry if missing
                                if field_name == "color" and not val:
                                    val = target_obj._meta.get_field("color").get_default()
                                if field_name == "stroke" and not val:
                                    val = target_obj._meta.get_field("stroke").get_default()

                                setattr(target_obj, field_name, val)

                            # Set the traceability field
                            if hasattr(target_obj, copied_field):
                                setattr(target_obj, copied_field, source_obj)

                            target_obj.save()
                            cloned_count += 1

                        except Exception as exc:
                            logger.exception(
                                f"Failed to clone {sect_code} ({ocr_model.__name__}) from OCR "
                                f"{ocr.pk} to OCC {occurrence.pk}"
                            )
                            errors_details.append(
                                {
                                    "migrated_from_id": getattr(ocr, "migrated_from_id", ""),
                                    "column": f"CLONE-{sect_code}",
                                    "level": "error",
                                    "message": f"Failed to clone {sect_code} to Occurrence {occurrence.pk}: {exc}",
                                    "raw_value": str(getattr(ocr, "pk", "")),
                                }
                            )

            logger.info(f"Finished populating Occurrence 1-to-1 objects. Cloned {cloned_count} sections.")

        persist_end = timezone.now()
        persist_duration = persist_end - transform_end

        # Add per-phase timings to stats for more accurate reporting
        stats["time_extract"] = str(extract_duration)
        stats["time_transform"] = str(transform_duration)
        stats["time_persist"] = str(persist_duration)

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        stats["error_count_details"] = len(errors_details)
        stats["warning_count_details"] = len(warnings_details)
        stats["warning_messages"] = warnings
        stats["error_details_csv"] = None

        # Append global warnings to errors_details if any, so they appear in CSV
        for w_msg in warnings:
            # simple parse specific to how we append these warnings
            level = "warning"
            source_ref = ""
            msg_body = w_msg
            if ": " in w_msg:
                # "source: message"
                parts = w_msg.split(": ", 1)
                source_ref = parts[0]
                msg_body = parts[1]

            errors_details.append(
                {
                    "migrated_from_id": "",
                    "column": "",
                    "level": level,
                    "message": msg_body,
                    "raw_value": "",
                    "reason": source_ref,
                    "row": {},
                }
            )

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)

        if errors_details:
            csv_path = options.get("error_csv")
            if csv_path:
                csv_path = os.path.abspath(csv_path)
            else:
                ts = timezone.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(
                    os.getcwd(),
                    "private-media/handler_output",
                    f"{self.slug}_errors_{ts}.csv",
                )
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
                    import csv

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
                stats["error_details_csv"] = csv_path
                logger.info(
                    (
                        "OccurrenceReportImporter %s finished; processed=%d created=%d "
                        "updated=%d skipped=%d errors=%d warnings=%d time_taken=%s (details -> %s)"
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
                logger.error("Failed to write error CSV for %s at %s: %s", self.slug, csv_path, e)
                logger.info(
                    (
                        "OccurrenceReportImporter %s finished; processed=%d created=%d "
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
            logger.info(
                (
                    "OccurrenceReportImporter %s finished; processed=%d created=%d updated=%d"
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
