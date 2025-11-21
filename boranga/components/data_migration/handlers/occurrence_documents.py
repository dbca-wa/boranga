from __future__ import annotations

from typing import Any

from django.apps import apps
from django.db import transaction

from boranga.components.data_migration.adapters.occurrence_documents import schema
from boranga.components.data_migration.adapters.occurrence_documents.tpfl import (
    OccurrenceDocumentTpflAdapter,
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
    Source.TPFL.value: OccurrenceDocumentTpflAdapter(),
    # Add new adapters here as implemented
}


@register
class OccurrenceDocumentImporter(BaseSheetImporter):
    slug = "occurrence_documents_legacy"
    description = (
        "Import occurrence child documents from legacy TPFL/TEC/TFAUNA sources"
    )

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete OccurrenceDocument target data. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            return

        logger = __import__("logging").getLogger(__name__)
        logger.warning(
            "OccurrenceDocumentImporter: deleting OccurrenceDocument data..."
        )
        from django.apps import apps
        from django.db import transaction

        with transaction.atomic():
            try:
                OccurrenceDocument = apps.get_model("occurrence", "OccurrenceDocument")
                OccurrenceDocument.objects.all().delete()
            except Exception:
                logger.exception("Failed to delete OccurrenceDocument")

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

        # 2. Build pipelines from adapter/schema
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

        # 3. Transform each row (documents are 1:many children; no merging)
        transformed_rows: list[tuple[dict, str, list[tuple[str, Any]]]] = []
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
            transformed_rows.append((transformed, row.get("_source"), issues))

        # 4. Persist each transformed document row, linking to parent occurrence
        # Resolve models
        try:
            Occurrence = apps.get_model("occurrence", "Occurrence")
        except LookupError:
            raise RuntimeError("Model occurrence.Occurrence not found")

        try:
            OccurrenceDocument = apps.get_model("occurrence", "OccurrenceDocument")
        except LookupError:
            # If OccurrenceDocument model is not present, we cannot persist documents
            raise RuntimeError("Model occurrence.OccurrenceDocument not found")

        # helper to filter payload to model fields
        def filter_fields(model, payload: dict):
            allowed = {
                f.name
                for f in model._meta.get_fields()
                if f.concrete and not f.many_to_many and not f.one_to_many
            }
            return {k: v for k, v in payload.items() if k in allowed}

        for transformed, src, issues in transformed_rows:
            # normalized key: pipelines for occurrence_id may return occurrence instance or migrated id
            occurrence_id = transformed.get("occurrence_id")
            if occurrence_id is None:
                warnings.append(f"{src}: missing occurrence_id after transform")
                skipped += 1
                errors += 1
                continue

            # locate parent occurrence instance
            occ_obj = None

            try:
                occ_obj = Occurrence.objects.get(pk=str(occurrence_id))
            except Occurrence.DoesNotExist:
                warnings.append(f"{src}: occurrence_id {occurrence_id} not found")
                occ_obj = None

            if occ_obj is None:
                warnings.append(
                    f"{src}: cannot link document to occurrence {occurrence_id}"
                )
                skipped += 1
                errors += 1
                continue

            # build payload mapping to model fields
            payload = {
                "occurrence": occ_obj,
                "document_category_id": transformed.get("document_category_id"),
                "document_sub_category_id": transformed.get("document_sub_category_id"),
                "uploaded_by": transformed.get("uploaded_by"),
                "uploaded_date": transformed.get("uploaded_date"),
                "description": transformed.get("description"),
            }

            # filter fields that actually exist on the model
            safe_payload = filter_fields(OccurrenceDocument, payload)

            if ctx.dry_run:
                # don't persist
                continue

            # Determine lookup for update_or_create: prefer a combination that uniquely identifies a doc
            lookup = {}
            for key in (
                "occurrence",
                "document_category",
                "document_sub_category",
                "uploaded_by",
                "uploaded_date",
            ):
                if key in safe_payload:
                    lookup[key] = safe_payload[key]
            try:
                with transaction.atomic():
                    if lookup:
                        obj, created_flag = OccurrenceDocument.objects.update_or_create(
                            defaults=safe_payload, **lookup
                        )
                    else:
                        OccurrenceDocument.objects.create(**safe_payload)
                        created_flag = True
                    if created_flag:
                        created += 1
                    else:
                        updated += 1
            except Exception:
                # don't fail whole run for one bad row
                skipped += 1
                errors += 1
                continue

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        return stats
