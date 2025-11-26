from __future__ import annotations

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
from boranga.components.occurrence.models import (
    AssociatedSpeciesTaxonomy,
    OccurrenceReport,
    OCRAssociatedSpecies,
    OCRHabitatComposition,
    OCRHabitatCondition,
    OCRIdentification,
    OCRObserverDetail,
)
from boranga.components.species_and_communities.models import Taxonomy, TaxonVernacular

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
    # add other adapters when available
}


@register
class OccurrenceReportImporter(BaseSheetImporter):
    slug = "occurrence_report_legacy"
    description = "Import occurrence reports from legacy sources (TPFL etc)"

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete OccurrenceReport target data and its child tables. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            logger.info(
                "OccurrenceReportImporter.clear_targets: dry-run, skipping delete"
            )
            return

        logger.warning(
            "OccurrenceReportImporter: deleting OccurrenceReport and related data..."
        )

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
                OccurrenceReport.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OccurrenceReport")

            # Reset the primary key sequence for OccurrenceReport when using PostgreSQL.
            try:
                if getattr(conn, "vendor", None) == "postgresql":
                    table = OccurrenceReport._meta.db_table
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT setval(pg_get_serial_sequence(%s, %s), %s, %s)",
                            [table, "id", 1, False],
                        )
                    logger.info("Reset primary key sequence for table %s", table)
            except Exception:
                logger.exception(
                    "Failed to reset OccurrenceReport primary key sequence"
                )
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
            "OccurrenceReportImporter (%s) started at %s (dry_run=%s)",
            self.slug,
            start_time.isoformat(),
            ctx.dry_run,
        )

        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

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
        groups: dict[str, list[tuple[dict, str, list[tuple[str, Any]]]]] = defaultdict(
            list
        )

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
            pipeline_map = pipelines_by_source.get(
                src, pipelines_by_source.get(None, {})
            )
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
                key=lambda e: (
                    source_priority.index(e[1])
                    if e[1] in source_priority
                    else len(source_priority)
                ),
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
                    errors += sum(
                        1 for level, _ in validation_issues if level == "error"
                    )
                    continue

            defaults = report_row.to_model_defaults()

            # Ensure `reported_date` is populated when missing by copying
            # from `lodgement_date`. The schema treats `reported_date` as a
            # copy of `lodgement_date` but the TPFL pipelines only produce
            # `lodgement_date`, so fill it here to avoid NULLs for the
            # model's non-nullable `reported_date` field.
            if (
                defaults.get("reported_date") is None
                and defaults.get("lodgement_date") is not None
            ):
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
                if not getattr(field, "null", False) and isinstance(
                    field, (dj_models.CharField, dj_models.TextField)
                ):
                    defaults[k] = ""
                    continue

            if ctx.dry_run:
                # Avoid emitting extremely large JSON blobs to the logger which
                # can make the process appear to hang when many or very large
                # records are processed. Produce a truncated preview instead.
                try:
                    pretty = json.dumps(defaults, default=str, indent=2, sort_keys=True)
                    if len(pretty) > 2000:
                        preview = pretty[
                            :2000
                        ] + "\n... (truncated, total %d chars)" % (len(pretty))
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
                # Also show a concise associated-species preview for this OCR
                try:
                    # load a small sample of the mapping (cheap)
                    sample_map = load_sheet_associated_species_names(
                        path, max_lines=200
                    )
                    # Attempt several key variants so we reliably match mapping keys
                    names = []
                    if sample_map:
                        key_variants = [migrated_from_id]
                        try:
                            key_variants.append(str(migrated_from_id))
                        except Exception:
                            pass
                        try:
                            key_variants.append(int(migrated_from_id))
                        except Exception:
                            pass
                        for k in key_variants:
                            if k in sample_map:
                                names = sample_map.get(k) or []
                                break

                    # Always log a concise per-OCR associated-species preview so
                    # it appears immediately after the OCR defaults preview.
                    if not names:
                        logger.debug(
                            "OccurrenceReportImporter %s dry-run: associated-species for "
                            "migrated_from_id=%s: (no mapping found)",
                            self.slug,
                            migrated_from_id,
                        )
                    else:
                        # Bulk-resolve the small sample of names to avoid per-name
                        # DB queries. Mirrors the bulk-resolution logic used later.
                        assoc_lines = []
                        sample_names = [n for n in names[:200] if n]
                        name_to_tax_samp = {}
                        if sample_names:
                            from django.db.models.functions import Lower

                            sample_lower = {n.casefold() for n in sample_names}
                            taxa_qs_samp = Taxonomy.objects.annotate(
                                _ln=Lower("scientific_name")
                            ).filter(_ln__in=list(sample_lower))
                            taxa_map_samp = {
                                t.scientific_name.casefold(): t for t in taxa_qs_samp
                            }

                            vern_qs_samp = (
                                TaxonVernacular.objects.annotate(
                                    _ln=Lower("vernacular_name")
                                )
                                .filter(_ln__in=list(sample_lower))
                                .select_related("taxonomy")
                            )
                            vern_map_samp = {
                                v.vernacular_name.casefold(): v for v in vern_qs_samp
                            }

                            for name in sample_names:
                                ln = name.casefold()
                                tax = taxa_map_samp.get(ln)
                                if not tax:
                                    tv = vern_map_samp.get(ln)
                                    tax = tv.taxonomy if tv else None
                                if tax:
                                    name_to_tax_samp[name] = tax

                        for n in names[:20]:
                            if not n:
                                assoc_lines.append(f"{n} -> unresolved")
                                continue
                            tax = None
                            if sample_names and n in name_to_tax_samp:
                                tax = name_to_tax_samp.get(n)
                            else:
                                # fallback to original behaviour for rare names
                                tax = Taxonomy.objects.filter(
                                    scientific_name__iexact=n
                                ).first()
                                if not tax:
                                    tv = (
                                        TaxonVernacular.objects.filter(
                                            vernacular_name__iexact=n
                                        )
                                        .select_related("taxonomy")
                                        .first()
                                    )
                                    tax = tv.taxonomy if tv else None
                            if tax:
                                ast = AssociatedSpeciesTaxonomy.objects.filter(
                                    taxonomy=tax
                                ).first()
                                if ast:
                                    assoc_lines.append(
                                        f"{n} -> assoc_id={ast.pk}, tax_id={tax.pk}"
                                    )
                                else:
                                    assoc_lines.append(
                                        f"{n} -> taxonomy_id={tax.pk} (assoc missing)"
                                    )
                            else:
                                assoc_lines.append(f"{n} -> unresolved")
                        logger.debug(
                            "OccurrenceReportImporter %s dry-run: associated-species for migrated_from_id=%s: %s",
                            self.slug,
                            migrated_from_id,
                            "; ".join(assoc_lines),
                        )
                except Exception:
                    logger.exception(
                        "Failed to build associated-species preview for migrated_from_id=%s",
                        migrated_from_id,
                    )
                continue

            # capture related small extras for later (observer + habitat)
            # collect all OCRHabitatComposition__* keys into a habitat_data dict
            habitat_data = {}
            identification_data = {}
            habitat_condition = {}
            for k, v in merged.items():
                if k.startswith("OCRHabitatComposition__"):
                    short = k.split("OCRHabitatComposition__", 1)[1]
                    habitat_data[short] = v
                if k.startswith("OCRHabitatCondition__"):
                    short = k.split("OCRHabitatCondition__", 1)[1]
                    habitat_condition[short] = v
                if k.startswith("OCRIdentification__"):
                    short = k.split("OCRIdentification__", 1)[1]
                    identification_data[short] = v

            ops.append(
                {
                    "migrated_from_id": migrated_from_id,
                    "defaults": defaults,
                    "merged": merged,
                    "habitat_data": habitat_data,
                    "habitat_condition": habitat_condition,
                    "identification_data": identification_data,
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

        # Build op_map for O(1) access to per-migrated-id data (avoid O(n) scans)
        op_map = {o["migrated_from_id"]: o for o in ops}

        # Prefetch existing OccurrenceReports to decide create vs update
        migrated_keys = [o["migrated_from_id"] for o in ops]
        existing_by_migrated = {
            s.migrated_from_id: s
            for s in OccurrenceReport.objects.filter(migrated_from_id__in=migrated_keys)
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

            obj = existing_by_migrated.get(migrated_from_id)
            if obj:
                # apply defaults to instance for later bulk_update
                for k, v in defaults.items():
                    apply_value_to_instance(obj, k, v)
                to_update.append((obj, habitat_data, habitat_condition))
                continue

            # create new instance (bulk_create later)
            create_kwargs = dict(defaults)
            create_kwargs["migrated_from_id"] = migrated_from_id
            if getattr(ctx, "migration_run", None) is not None:
                create_kwargs["migration_run"] = ctx.migration_run
            inst = OccurrenceReport(
                **normalize_create_kwargs(OccurrenceReport, create_kwargs)
            )
            to_create.append(inst)
            create_meta.append((migrated_from_id, habitat_data, habitat_condition))

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
            for s in OccurrenceReport.objects.filter(migrated_from_id__in=created_keys):
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
                    OccurrenceReport.objects.bulk_update(
                        occs_to_update, ["occurrence_report_number"], batch_size=BATCH
                    )
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
                        if all(
                            getattr(inst, f.name, None) is not None
                            for inst in update_instances
                        ):
                            fields.append(f.name)
                    except Exception:
                        # Be conservative: skip fields that raise on getattr
                        continue
            # perform bulk_update only if we have safe fields to update
            try:
                if fields:
                    OccurrenceReport.objects.bulk_update(
                        update_instances, fields, batch_size=BATCH
                    )
            except Exception:
                logger.exception(
                    "Failed to bulk_update OccurrenceReport; falling back to individual saves"
                )
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
                            if getattr(inst, f.name, None) is not None
                            and f.name not in ("id", "migrated_from_id")
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
        target_occs = list(
            OccurrenceReport.objects.filter(migrated_from_id__in=target_mig_ids)
        )
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
            sheet_to_species = load_sheet_associated_species_names(path)

        # If any mapping rows found, resolve names to AssociatedSpeciesTaxonomy
        if sheet_to_species:
            # Normalize sheet keys to strings and strip; ensure matching with
            # target_map keys which are strings from migrated_from_id.
            normalized_sheet_to_species: dict[str, list[str]] = {}
            for k, v in sheet_to_species.items():
                if k is None:
                    continue
                ks = str(k).strip()
                if not ks:
                    continue
                normalized_sheet_to_species[ks] = [str(n).strip() for n in v if n]
            sheet_to_species = normalized_sheet_to_species

            # unique species names
            uniq_names = {n for lst in sheet_to_species.values() for n in lst}

            logger.info(
                "OccurrenceReportImporter: resolving %d unique associated-species names",
                len(uniq_names),
            )

            # Batch-resolve Taxonomy by case-insensitive scientific_name and
            # TaxonVernacular by vernacular_name (both case-insensitive).
            from django.db.models.functions import Lower

            lower_names = {n.casefold() for n in uniq_names}

            taxa_qs = Taxonomy.objects.annotate(_ln=Lower("scientific_name")).filter(
                _ln__in=list(lower_names)
            )
            taxa_map = {t.scientific_name.casefold(): t for t in taxa_qs}

            vern_qs = (
                TaxonVernacular.objects.annotate(_ln=Lower("vernacular_name"))
                .filter(_ln__in=list(lower_names))
                .select_related("taxonomy")
            )
            vern_map = {v.vernacular_name.casefold(): v for v in vern_qs}

            # Resolve name -> taxonomy using scientific_name first, then vernacular
            name_to_tax: dict[str, Taxonomy] = {}
            unresolved = []
            for name in uniq_names:
                ln = name.casefold()
                tax = taxa_map.get(ln)
                if not tax:
                    tv = vern_map.get(ln)
                    tax = tv.taxonomy if tv else None
                if not tax:
                    unresolved.append(name)
                    continue
                name_to_tax[name] = tax

            if unresolved:
                logger.warning(
                    "OccurrenceReportImporter: %d associated-species names unresolved",
                    len(unresolved),
                )
                # record up to 20 unresolved examples in warnings for later inspection
                for ex in unresolved[:20]:
                    warnings.append(f"associated_species: no taxonomy match for '{ex}'")

            # Load existing AssociatedSpeciesTaxonomy rows for all resolved taxonomy ids
            tax_ids = {t.pk for t in name_to_tax.values()}
            ast_qs = AssociatedSpeciesTaxonomy.objects.filter(
                taxonomy__in=list(tax_ids)
            )
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
                    create_objs = [
                        AssociatedSpeciesTaxonomy(taxonomy_id=tid)
                        for tid in missing_tax_ids
                    ]
                    AssociatedSpeciesTaxonomy.objects.bulk_create(
                        create_objs, batch_size=BATCH
                    )
                    # Refresh created rows to ensure we have their PKs
                    for ast in AssociatedSpeciesTaxonomy.objects.filter(
                        taxonomy_id__in=list(missing_tax_ids)
                    ):
                        if ast.taxonomy_id not in taxid_to_ast:
                            taxid_to_ast[ast.taxonomy_id] = ast
                except Exception:
                    logger.exception(
                        "Bulk create failed for AssociatedSpeciesTaxonomy; trying individual creates"
                    )
                    created_asts = []
                    for tid in missing_tax_ids:
                        try:
                            created_asts.append(
                                AssociatedSpeciesTaxonomy.objects.create(
                                    taxonomy_id=tid
                                )
                            )
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
                for a in OCRAssociatedSpecies.objects.filter(
                    occurrence_report__in=target_occs
                ).prefetch_related("related_species")
            }

            # Create OCRAssociatedSpecies for occurrence reports that need them
            assoc_to_create = []
            for sheetno, names in sheet_to_species.items():
                ocr = target_map.get(sheetno)
                if not ocr:
                    continue
                if ocr.pk not in existing_assoc:
                    # create only if there are resolved species
                    resolved = [name_to_assoc[n] for n in names if n in name_to_assoc]
                    if not resolved:
                        continue
                    assoc = OCRAssociatedSpecies(occurrence_report=ocr)
                    assoc_to_create.append(assoc)

            if assoc_to_create:
                try:
                    OCRAssociatedSpecies.objects.bulk_create(
                        assoc_to_create, batch_size=BATCH
                    )
                except Exception:
                    logger.exception(
                        "Failed to bulk_create OCRAssociatedSpecies; falling back to individual saves"
                    )
                    for a in assoc_to_create:
                        try:
                            a.save()
                        except Exception:
                            logger.exception(
                                "Failed to create OCRAssociatedSpecies for occurrence_report %s",
                                getattr(a.occurrence_report, "pk", None),
                            )

            # Refresh existing_assoc mapping
            existing_assoc = {
                a.occurrence_report_id: a
                for a in OCRAssociatedSpecies.objects.filter(
                    occurrence_report__in=target_occs
                )
            }

            # Prepare through model info for bulk operations
            through = OCRAssociatedSpecies.related_species.through
            assoc_fk_field = None
            tax_fk_field = None
            for f in through._meta.get_fields():
                if (
                    getattr(f, "remote_field", None)
                    and getattr(f.remote_field, "model", None) == OCRAssociatedSpecies
                ):
                    assoc_fk_field = f.name
                if (
                    getattr(f, "remote_field", None)
                    and getattr(f.remote_field, "model", None)
                    == AssociatedSpeciesTaxonomy
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
                    desired_ids = {
                        name_to_assoc[n].pk for n in names if n in name_to_assoc
                    }
                    # existing related ids (prefetched so no DB hit per-obj)
                    existing_ids = set(
                        assoc_obj.related_species.values_list("id", flat=True)
                    )
                    add_ids = desired_ids - existing_ids
                    remove_ids = existing_ids - desired_ids
                    for aid in add_ids:
                        to_create_through.append(
                            through(**{assoc_fk_id: assoc_obj.pk, tax_fk_id: aid})
                        )
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
                            through.objects.bulk_create(
                                to_create_through[i : i + BATCH], batch_size=BATCH
                            )
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

        # OCRObserverDetail: ensure a main observer exists for each occurrence_report
        want_obs_create = []
        existing_obs = set(
            OCRObserverDetail.objects.filter(
                occurrence_report__in=target_occs, main_observer=True
            ).values_list("occurrence_report_id", flat=True)
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
            op = op_map.get(mig)
            if op:
                merged = op.get("merged") or {}
                observer_name = merged.get("OCRObserverDetail__observer_name")
                observer_role = merged.get("OCRObserverDetail__role")

            # create observer instance after searching ops so the variables
            # `observer_name` and `observer_role` are defined regardless of
            # whether the loop hit the break path
            ocr_observer_detail_instance = OCRObserverDetail(
                occurrence_report=ocr,
                main_observer=True,
                visible=True,
            )
            apply_value_to_instance(
                ocr_observer_detail_instance, "observer_name", observer_name
            )
            apply_value_to_instance(ocr_observer_detail_instance, "role", observer_role)

            want_obs_create.append(ocr_observer_detail_instance)

        if want_obs_create:
            try:
                OCRObserverDetail.objects.bulk_create(want_obs_create, batch_size=BATCH)
            except Exception:
                logger.exception(
                    "Failed to bulk_create OCRObserverDetail; falling back to individual creates"
                )
                for obj in want_obs_create:
                    try:
                        obj.save()
                    except Exception:
                        logger.exception(
                            "Failed to create OCRObserverDetail for occurrence_report %s",
                            getattr(obj.occurrence_report, "pk", None),
                        )

        # OCRHabitatComposition: OneToOne - create or update loose_rock_percent
        # Fetch existing habitat comps
        existing_habs = {
            h.occurrence_report_id: h
            for h in OCRHabitatComposition.objects.filter(
                occurrence_report__in=target_occs
            )
        }
        # Fetch existing habitat conditions
        existing_conds = {
            c.occurrence_report_id: c
            for c in OCRHabitatCondition.objects.filter(
                occurrence_report__in=target_occs
            )
        }
        # Fetch existing identifications
        existing_idents = {
            it.occurrence_report_id: it
            for it in OCRIdentification.objects.filter(
                occurrence_report__in=target_occs
            )
        }
        habs_to_create = []
        habs_to_update = []
        conds_to_create = []
        conds_to_update = []
        idents_to_create = []
        idents_to_update = []
        for up in to_update:
            inst, habitat_data, habitat_condition = up
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
                    OCRHabitatComposition(
                        **normalize_create_kwargs(OCRHabitatComposition, create_kwargs)
                    )
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
                conds_to_create.append(
                    OCRHabitatCondition(
                        **normalize_create_kwargs(OCRHabitatCondition, cond_create)
                    )
                )
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
                idents_to_create.append(
                    OCRIdentification(
                        **normalize_create_kwargs(OCRIdentification, create_kwargs)
                    )
                )

        # Handle created ones
        for mig, habitat_data, habitat_condition in create_meta:
            ocr = created_map.get(mig)
            if not ocr:
                continue
            hd = habitat_data or {}
            hc = habitat_condition or {}
            # also pull identification_data from create_meta mapping (create_meta entries are tuples of
            # (migrated_from_id, habitat_data, habitat_condition, identification_data) )
            # but create_meta was appended as (migrated_from_id, habitat_data, habitat_condition) earlier;
            # we need to find the op to get identification_data
            ident_data = {}
            ident_data = op_map.get(mig, {}).get("identification_data") or {}
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
                    OCRHabitatComposition(
                        **normalize_create_kwargs(OCRHabitatComposition, create_kwargs)
                    )
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
                conds_to_create.append(
                    OCRHabitatCondition(
                        **normalize_create_kwargs(OCRHabitatCondition, cond_create)
                    )
                )
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
                idents_to_create.append(
                    OCRIdentification(
                        **normalize_create_kwargs(OCRIdentification, create_kwargs)
                    )
                )

        if habs_to_create:
            try:
                OCRHabitatComposition.objects.bulk_create(
                    habs_to_create, batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_create OCRHabitatComposition; falling back to individual creates"
                )
                for h in habs_to_create:
                    try:
                        h.save()
                    except Exception:
                        logger.exception(
                            "Failed to create OCRHabitatComposition for occurrence_report %s",
                            getattr(h.occurrence_report, "pk", None),
                        )

        if habs_to_update:
            try:
                OCRHabitatComposition.objects.bulk_update(
                    habs_to_update, ["loose_rock_percent"], batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_update OCRHabitatComposition; falling back to individual saves"
                )
                for h in habs_to_update:
                    try:
                        h.save()
                    except Exception:
                        logger.exception(
                            "Failed to save OCRHabitatComposition %s",
                            getattr(h, "pk", None),
                        )

        # OCRHabitatCondition: OneToOne - create or update percentage flags
        if conds_to_create:
            try:
                OCRHabitatCondition.objects.bulk_create(
                    conds_to_create, batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_create OCRHabitatCondition; falling back to individual creates"
                )
                for c in conds_to_create:
                    try:
                        c.save()
                    except Exception:
                        logger.exception(
                            "Failed to create OCRHabitatCondition for occurrence_report %s",
                            getattr(c.occurrence_report, "pk", None),
                        )

        if conds_to_update:
            try:
                # determine fields to update from condition instances
                cond_fields = set()
                for inst in conds_to_update:
                    cond_fields.update(
                        [
                            f.name
                            for f in inst._meta.fields
                            if getattr(inst, f.name, None) is not None
                        ]
                    )
                # ensure occurrence_report_id or id not included
                cond_fields = {
                    f
                    for f in cond_fields
                    if f not in ("id", "occurrence_report", "occurrence_report_id")
                }
                if cond_fields:
                    OCRHabitatCondition.objects.bulk_update(
                        conds_to_update, list(cond_fields), batch_size=BATCH
                    )
            except Exception:
                logger.exception(
                    "Failed to bulk_update OCRHabitatCondition; falling back to individual saves"
                )
                for c in conds_to_update:
                    try:
                        c.save()
                    except Exception:
                        logger.exception(
                            "Failed to save OCRHabitatCondition %s",
                            getattr(c, "pk", None),
                        )

        # OCRIdentification: OneToOne - create or update identification records
        if idents_to_create:
            try:
                OCRIdentification.objects.bulk_create(
                    idents_to_create, batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_create OCRIdentification; falling back to individual creates"
                )
                for i in idents_to_create:
                    try:
                        i.save()
                    except Exception:
                        logger.exception(
                            "Failed to create OCRIdentification for occurrence_report %s",
                            getattr(i.occurrence_report, "pk", None),
                        )

        if idents_to_update:
            try:
                ident_fields = set()
                for inst in idents_to_update:
                    ident_fields.update(
                        [
                            f.name
                            for f in inst._meta.fields
                            if getattr(inst, f.name, None) is not None
                        ]
                    )
                # exclude id or FK reference
                ident_fields = {
                    f
                    for f in ident_fields
                    if f not in ("id", "occurrence_report", "occurrence_report_id")
                }
                if ident_fields:
                    OCRIdentification.objects.bulk_update(
                        idents_to_update, list(ident_fields), batch_size=BATCH
                    )
            except Exception:
                logger.exception(
                    "Failed to bulk_update OCRIdentification; falling back to individual saves"
                )
                for i in idents_to_update:
                    try:
                        i.save()
                    except Exception:
                        logger.exception(
                            "Failed to save OCRIdentification %s",
                            getattr(i, "pk", None),
                        )

        # Update stats counts for created/updated based on performed ops
        created += len(created_map)
        updated += len(to_update)

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
                    "boranga/components/data_migration/handlers/handler_output",
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
                logger.error(
                    "Failed to write error CSV for %s at %s: %s", self.slug, csv_path, e
                )
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
