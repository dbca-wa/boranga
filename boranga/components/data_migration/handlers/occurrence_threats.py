from __future__ import annotations

from typing import Any

from django.apps import apps
from django.db import transaction

from boranga.components.data_migration.adapters.occurrence_threats import schema
from boranga.components.data_migration.adapters.occurrence_threats.tpfl import (
    OccurrenceThreatTpflAdapter,
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
    Source.TPFL.value: OccurrenceThreatTpflAdapter(),
}


@register
class OccurrenceThreatImporter(BaseSheetImporter):
    slug = "occurrence_threats_legacy"
    description = "Import occurrence threats from legacy TPFL sources (SHEETNO=Null)"

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete OCCConservationThreat target data (only those without report link). Respect `ctx.dry_run`."""
        if ctx.dry_run:
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

        logger = __import__("logging").getLogger(__name__)

        if is_filtered:
            if not target_group_types:
                return
            logger.warning(
                "OccurrenceThreatImporter: deleting OCCConservationThreat data "
                "(where occurrence_report_threat is NULL) for group_types: %s ...",
                target_group_types,
            )
            threat_filter = {
                "occurrence_report_threat__isnull": True,
                "occurrence__group_type__name__in": target_group_types,
            }
        else:
            logger.warning(
                "OccurrenceThreatImporter: deleting OCCConservationThreat data "
                "(where occurrence_report_threat is NULL)..."
            )
            threat_filter = {"occurrence_report_threat__isnull": True}

        from django.apps import apps
        from django.db import connections

        conn = connections["default"]
        was_autocommit = conn.get_autocommit()
        if not was_autocommit:
            conn.set_autocommit(True)

        try:
            OCCConservationThreat = apps.get_model("boranga", "OCCConservationThreat")
            try:
                # Only delete threats that are not linked to a report threat (i.e. direct legacy threats)
                OCCConservationThreat.objects.filter(**threat_filter).delete()
            except Exception:
                logger.exception("Failed to delete OCCConservationThreat")

            # Reset the primary key sequence for OCCConservationThreat when using PostgreSQL.
            try:
                if getattr(conn, "vendor", None) == "postgresql":
                    table = OCCConservationThreat._meta.db_table
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
                    logger.info(
                        "Reset primary key sequence for table %s to %s", table, max_id
                    )
            except Exception:
                logger.exception(
                    "Failed to reset OCCConservationThreat primary key sequence"
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
        logger = __import__("logging").getLogger(__name__)
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

        # Apply optional global per-importer limit (ctx.limit) after extraction
        limit = getattr(ctx, "limit", None)
        if limit:
            try:
                all_rows = all_rows[: int(limit)]
            except Exception:
                pass

        # 2. Build pipelines
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

        errors_details = []

        # 3. Transform
        transformed_rows: list[tuple[dict, str, list[tuple[str, Any]], dict]] = []
        for row in all_rows:
            processed += 1
            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False

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
                                "migrated_from_id": row.get("migrated_from_id")
                                or row.get("occurrence_id"),
                                "column": col,
                                "level": issue.level,
                                "message": issue.message,
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
            transformed_rows.append((transformed, row.get("_source"), issues, row))

        # 4. Persist
        try:
            Occurrence = apps.get_model("boranga", "Occurrence")
            OCCConservationThreat = apps.get_model("boranga", "OCCConservationThreat")
        except LookupError:
            raise RuntimeError("Required models not found")

        valid_occurrence_ids = set(Occurrence.objects.values_list("pk", flat=True))

        to_create = []

        for transformed, src, issues, row in transformed_rows:
            occurrence_id = transformed.get("occurrence_id")
            if occurrence_id is None:
                warnings.append(f"{src}: missing occurrence_id after transform")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("migrated_from_id")
                        or row.get("occurrence_id"),
                        "column": "occurrence_id",
                        "level": "error",
                        "message": "missing occurrence_id after transform",
                        "raw_value": None,
                        "reason": "validation_error",
                        "row": row,
                    }
                )
                continue

            try:
                occ_id_val = int(occurrence_id)
            except (ValueError, TypeError):
                warnings.append(f"{src}: invalid occurrence_id format {occurrence_id}")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("migrated_from_id")
                        or row.get("occurrence_id"),
                        "column": "occurrence_id",
                        "level": "error",
                        "message": f"invalid occurrence_id format {occurrence_id}",
                        "raw_value": occurrence_id,
                        "reason": "validation_error",
                        "row": row,
                    }
                )
                continue

            if occ_id_val not in valid_occurrence_ids:
                warnings.append(f"{src}: occurrence_id {occurrence_id} not found")
                skipped += 1
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": row.get("migrated_from_id")
                        or row.get("occurrence_id"),
                        "column": "occurrence_id",
                        "level": "error",
                        "message": f"occurrence_id {occurrence_id} not found",
                        "raw_value": occurrence_id,
                        "reason": "validation_error",
                        "row": row,
                    }
                )
                continue

            payload = {
                "occurrence_id": occ_id_val,
                "occurrence_report_threat_id": None,  # Explicitly None
                "threat_category_id": transformed.get("threat_category_id"),
                "threat_agent_id": transformed.get("threat_agent_id"),
                "current_impact_id": transformed.get("current_impact_id"),
                "potential_impact_id": transformed.get("potential_impact_id"),
                "potential_threat_onset_id": transformed.get(
                    "potential_threat_onset_id"
                ),
                "comment": transformed.get("comment") or "",
                "date_observed": transformed.get("date_observed"),
                "visible": transformed.get("visible", True),
            }

            to_create.append((OCCConservationThreat(**payload), row))

        if ctx.dry_run:
            stats.update(
                processed=processed,
                created=len(to_create),
                updated=0,
                skipped=skipped,
                errors=errors,
                warnings=warn_count,
            )
            return stats

        # Bulk create
        if to_create:
            logger.info(f"Creating {len(to_create)} threats...")
            instances = [x[0] for x in to_create]

            try:
                with transaction.atomic():
                    created_objs = OCCConservationThreat.objects.bulk_create(
                        instances, batch_size=1000
                    )
                    created = len(created_objs)

                    # Post-process to set threat_number
                    for i, obj in enumerate(created_objs):
                        obj.threat_number = f"T{obj.pk}"

                    OCCConservationThreat.objects.bulk_update(
                        created_objs,
                        ["threat_number"],
                        batch_size=1000,
                    )

            except Exception as e:
                logger.error(
                    f"Bulk create failed ({e}), falling back to individual saves..."
                )
                for instance, row in to_create:
                    try:
                        instance.save()
                        created += 1

                        # Update threat_number
                        instance.threat_number = f"T{instance.pk}"
                        instance.save(update_fields=["threat_number"])

                    except Exception as inner_e:
                        skipped += 1
                        errors += 1
                        errors_details.append(
                            {
                                "migrated_from_id": row.get("migrated_from_id")
                                or row.get("occurrence_id"),
                                "column": None,
                                "level": "error",
                                "message": str(inner_e),
                                "raw_value": None,
                                "reason": "create_error",
                                "row": row,
                            }
                        )
                        continue

        # Write error CSV
        if errors_details:
            import csv
            import json
            import os

            from django.utils import timezone

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
                logger.warning(f"Wrote {len(errors_details)} errors to {csv_path}")
            except Exception:
                logger.exception("Failed to write error CSV")

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        return stats
