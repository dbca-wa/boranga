from __future__ import annotations

import csv
import json
import os
from typing import Any

from django.db import transaction
from django.utils import timezone

from boranga.components.data_migration.adapters.occurrence_report import schema
from boranga.components.data_migration.adapters.occurrence_report.threats import (
    OCRConservationThreatAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)

SOURCE_ADAPTERS = {
    Source.TPFL.value: OCRConservationThreatAdapter(),
}


@register
class OCRConservationThreatImporter(BaseSheetImporter):
    slug = "occurrence_report_threats_legacy"
    description = "Import occurrence report threats from legacy TPFL sources"

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete OCRConservationThreat target data. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            return

        logger = __import__("logging").getLogger(__name__)
        logger.warning(
            "OCRConservationThreatImporter: deleting OCRConservationThreat data..."
        )
        from django.db import transaction

        with transaction.atomic():
            try:
                from boranga.components.occurrence.models import OCRConservationThreat

                OCRConservationThreat.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OCRConservationThreat")

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
        errors_details = []

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

        # 2. Build pipelines per-source
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

        processed = 0
        errors = 0
        created = 0
        updated = 0
        skipped = 0
        warn_count = 0

        # 3. Transform each row
        transformed_rows: list[tuple[dict, str, list[tuple[str, Any]]]] = []
        for row in all_rows:
            processed += 1
            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            # Choose pipeline map based on row source (fallback to base)
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
                    if issue.level == "error":
                        has_error = True
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": row.get("SHEETNO"),
                                "column": col,
                                "level": "error",
                                "message": getattr(issue, "message", str(issue)),
                                "raw_value": raw_val,
                                "reason": "transform_error",
                                "row": row,
                            }
                        )
                    else:
                        warn_count += 1
            if has_error:
                skipped += 1
                continue
            transformed_rows.append((transformed, row.get("_source"), issues))

        # 4. Persist each transformed row
        try:
            from boranga.components.occurrence.models import OCRConservationThreat
        except ImportError:
            raise RuntimeError("Model occurrence.OCRConservationThreat not found")

        # helper to filter payload to model fields
        def filter_fields(model, payload: dict):
            allowed = {
                f.name
                for f in model._meta.get_fields()
                if f.concrete and not f.many_to_many and not f.one_to_many
            }
            return {k: v for k, v in payload.items() if k in allowed}

        to_create = []

        for transformed, src, issues in transformed_rows:
            # occurrence_report is FK
            occurrence_report_id = transformed.get("occurrence_report")
            if occurrence_report_id is None:
                warnings.append(f"{src}: missing occurrence_report after transform")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": transformed.get("occurrence_report"),
                        "column": "occurrence_report",
                        "level": "error",
                        "message": "missing occurrence_report after transform",
                        "raw_value": None,
                        "reason": "missing_fk",
                        "row": transformed,
                    }
                )
                continue

            # build payload mapping to model fields
            payload = {
                "occurrence_report_id": occurrence_report_id,
                "threat_category_id": transformed.get("threat_category"),
                "threat_agent_id": transformed.get("threat_agent"),
                "current_impact_id": transformed.get("current_impact"),
                "potential_impact_id": transformed.get("potential_impact"),
                "potential_threat_onset_id": transformed.get("potential_threat_onset"),
                "comment": transformed.get("comment"),
                "date_observed": transformed.get("date_observed"),
                "visible": transformed.get("visible"),
            }

            # filter fields that actually exist on the model
            safe_payload = filter_fields(OCRConservationThreat, payload)
            to_create.append(OCRConservationThreat(**safe_payload))

        if not ctx.dry_run and to_create:
            try:
                with transaction.atomic():
                    # Use batch_size to avoid huge SQL queries
                    created_objs = OCRConservationThreat.objects.bulk_create(
                        to_create, batch_size=1000
                    )
                    created = len(created_objs)

                    # Post-process to set threat_number (mimic save() method)
                    for obj in created_objs:
                        obj.threat_number = f"T{obj.pk}"

                    OCRConservationThreat.objects.bulk_update(
                        created_objs, ["threat_number"], batch_size=1000
                    )

            except Exception as e:
                logger = __import__("logging").getLogger(__name__)
                logger.exception("Bulk create failed")
                skipped += len(to_create)
                errors += len(to_create)
                errors_details.append(
                    {
                        "migrated_from_id": "BATCH",
                        "column": None,
                        "level": "error",
                        "message": f"Bulk create failed: {e}",
                        "raw_value": None,
                        "reason": "persist_error",
                        "row": {},
                    }
                )

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )

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
            except Exception as e:
                warnings.append(f"Failed to write error CSV: {e}")

        return stats
