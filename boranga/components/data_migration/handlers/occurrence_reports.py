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
from boranga.components.data_migration.adapters.occurrence_report.tpfl import (
    OccurrenceReportTpflAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.occurrence.models import (
    OccurrenceReport,
    OCRHabitatComposition,
    OCRHabitatCondition,
    OCRIdentification,
    OCRObserverDetail,
)

logger = logging.getLogger(__name__)

SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceReportTpflAdapter(),
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
        from django.db import transaction

        with transaction.atomic():
            try:
                OccurrenceReport.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OccurrenceReport")

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

        # Helper to normalize kwargs for creating model instances: if a field
        # is a ForeignKey and the value is an id (int/str), emit '<field>_id'
        # instead of the relation attribute so Django accepts it without a
        # model instance. Also helper to apply values to existing instances.
        def normalize_create_kwargs(model_cls, kwargs: dict) -> dict:
            out = {}
            # attempt to import MultiSelectField class if available
            try:
                from multiselectfield.db.fields import MultiSelectField

                _MSF = MultiSelectField
            except Exception:
                _MSF = None
            for k, v in (kwargs or {}).items():
                # If caller already provided a '<field>_id' key, keep it as-is
                if k.endswith("_id"):
                    out[k] = v
                    continue
                try:
                    f = model_cls._meta.get_field(k)
                except FieldDoesNotExist:
                    out[k] = v
                    continue
                if isinstance(f, dj_models.ForeignKey):
                    # map FK field name -> '<field>_id' so Django accepts the id
                    # If caller passed a model instance, unwrap to its PK.
                    out_val = getattr(v, "pk", v)
                    out[f"{k}_id"] = out_val
                else:
                    # Coerce MultiSelectField values: if field expects a
                    # multiselect and we received a scalar/string, convert
                    # to an iterable list. This avoids DB errors when the
                    # multiselect field tries to join a non-iterable.
                    if _MSF is not None and isinstance(f, _MSF):
                        if v is None:
                            out[k] = v
                        elif isinstance(v, str):
                            # split comma-separated string into list
                            out[k] = [s.strip() for s in v.split(",") if s.strip()]
                        elif isinstance(v, (list, tuple, set)):
                            out[k] = [str(x) for x in v]
                        else:
                            out[k] = [str(v)]
                    else:
                        out[k] = v
            return out

        def apply_value_to_instance(inst, field_name: str, val: Any):
            try:
                f = inst._meta.get_field(field_name)
            except FieldDoesNotExist:
                # If field_name ends with '_id' or field not found, set attribute directly
                setattr(inst, field_name, val)
                return
            # attempt to import MultiSelectField class if available
            try:
                from multiselectfield.db.fields import MultiSelectField

                _MSF = MultiSelectField
            except Exception:
                _MSF = None
            # If caller passed '<field>_id', set that attribute directly
            if field_name.endswith("_id"):
                setattr(inst, field_name, getattr(val, "pk", val))
                return
            if isinstance(f, dj_models.ForeignKey):
                setattr(inst, f"{field_name}_id", getattr(val, "pk", val))
            else:
                # For MultiSelectField, ensure the value is iterable (list)
                if _MSF is not None and isinstance(f, _MSF):
                    if val is None:
                        setattr(inst, field_name, val)
                        return
                    if isinstance(val, str):
                        val_list = [s.strip() for s in val.split(",") if s.strip()]
                        setattr(inst, field_name, val_list)
                        return
                    if isinstance(val, (list, tuple, set)):
                        setattr(inst, field_name, [str(x) for x in val])
                        return
                    # scalar -> wrap in list and coerce to str
                    setattr(inst, field_name, [str(val)])
                    return
                setattr(inst, field_name, val)

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
                pretty = json.dumps(defaults, default=str, indent=2, sort_keys=True)
                logger.debug(
                    "OccurrenceReportImporter %s dry-run: would persist migrated_from_id=%s defaults:\n%s",
                    self.slug,
                    migrated_from_id,
                    pretty,
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
            # determine fields to update from defaults keys (take union)
            fields = set()
            for inst, _ in to_update:
                fields.update(
                    [
                        f.name
                        for f in inst._meta.fields
                        if getattr(inst, f.name, None) is not None
                    ]
                )
            # ensure migrated_from_id not touched here
            try:
                OccurrenceReport.objects.bulk_update(
                    update_instances, list(fields), batch_size=BATCH
                )
            except Exception:
                logger.exception(
                    "Failed to bulk_update OccurrenceReport; falling back to individual saves"
                )
                for inst in update_instances:
                    try:
                        inst.save()
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

        # OCRObserverDetail: ensure a main observer exists for each occurrence_report
        want_obs_create = []
        existing_obs = set(
            OCRObserverDetail.objects.filter(
                occurrence_report__in=target_occs, main_observer=True
            ).values_list("occurrence_report_id", flat=True)
        )
        for mig in target_mig_ids:
            occ = target_map.get(mig)
            if not occ:
                continue
            if occ.pk in existing_obs:
                # already has main observer
                continue
            # find merged data for this migrated id to populate observer_name
            observer_name = None
            for o in ops:
                if o.get("migrated_from_id") == mig:
                    merged = o.get("merged") or {}
                    observer_name = merged.get("OCRObserverDetail__observer_name")
                    break

            want_obs_create.append(
                OCRObserverDetail(
                    occurrence_report=occ,
                    observer_name=observer_name,
                    main_observer=True,
                    visible=True,
                )
            )

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
            if occ.pk in existing_habs:
                h = existing_habs[occ.pk]
                valid_fields = {f.name for f in OCRHabitatComposition._meta.fields}
                for field_name, val in hd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fields:
                        apply_value_to_instance(h, field_name, val)
                habs_to_update.append(h)
            else:
                create_kwargs = {"occurrence_report": occ}
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
                habs_to_create.append(
                    OCRHabitatComposition(
                        **normalize_create_kwargs(OCRHabitatComposition, create_kwargs)
                    )
                )
            # OCRHabitatCondition handling for updates
            if occ.pk in existing_conds:
                c = existing_conds[occ.pk]
                valid_c_fields = {f.name for f in OCRHabitatCondition._meta.fields}
                for field_name, val in hc.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_c_fields:
                        apply_value_to_instance(c, field_name, val)
                conds_to_update.append(c)
            else:
                cond_create = {"occurrence_report": occ}
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
                for o in ops:
                    if o.get("migrated_from_id") == mig_key:
                        ident_data = o.get("identification_data") or {}
                        break

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
            occ = created_map.get(mig)
            if not occ:
                continue
            hd = habitat_data or {}
            hc = habitat_condition or {}
            # also pull identification_data from create_meta mapping (create_meta entries are tuples of
            # (migrated_from_id, habitat_data, habitat_condition, identification_data) )
            # but create_meta was appended as (migrated_from_id, habitat_data, habitat_condition) earlier;
            # we need to find the op to get identification_data
            ident_data = {}
            for o in ops:
                if o.get("migrated_from_id") == mig:
                    ident_data = o.get("identification_data") or {}
                    break
            if occ.pk in existing_habs:
                h = existing_habs[occ.pk]
                valid_fields = {f.name for f in OCRHabitatComposition._meta.fields}
                for field_name, val in hd.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_fields:
                        apply_value_to_instance(h, field_name, val)
                habs_to_update.append(h)
            else:
                create_kwargs = {"occurrence_report": occ}
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
            # OCRHabitatCondition create/update for newly created occ
            if occ.pk in existing_conds:
                c = existing_conds[occ.pk]
                valid_c_fields = {f.name for f in OCRHabitatCondition._meta.fields}
                for field_name, val in hc.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_c_fields:
                        apply_value_to_instance(c, field_name, val)
                conds_to_update.append(c)
            else:
                cond_create = {"occurrence_report": occ}
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
            # identification create for newly created occ
            if occ.pk in existing_idents:
                id_obj = existing_idents[occ.pk]
                valid_i_fields = {f.name for f in OCRIdentification._meta.fields}
                for field_name, val in ident_data.items():
                    if field_name == "occurrence_report":
                        continue
                    if val is not None and field_name in valid_i_fields:
                        apply_value_to_instance(id_obj, field_name, val)
                idents_to_update.append(id_obj)
            else:
                create_kwargs = {"occurrence_report": occ}
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
