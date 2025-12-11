from __future__ import annotations

import logging
import time

from django.utils import timezone
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.data_migration.adapters.base import (
    ExtractionResult,
    SourceAdapter,
)
from boranga.components.data_migration.adapters.occurrence import schema
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    build_legacy_map_transform,
    register,
    run_pipeline,
)
from boranga.components.occurrence.models import Occurrence, OccurrenceTenure
from boranga.components.spatial.models import TileLayer
from boranga.components.spatial.utils import (
    intersect_geometry_with_layer,
    populate_occurrence_tenure_data,
)

logger = logging.getLogger(__name__)

PURPOSE_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "PURPOSE (Code_Boranga_Match)",
    required=False,
)

VESTING_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "VESTING (Code_Boranga_Match)",
    required=False,
)

PIPELINES = {
    "migrated_from_id": ["strip", "required"],
    "OccurrenceTenure__purpose_id": ["strip", "blank_to_none", PURPOSE_TRANSFORM],
    "OccurrenceTenure__vesting_id": ["strip", "blank_to_none", VESTING_TRANSFORM],
}


class OccurrenceTenureAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "occurrence_tenure"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings = []
        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            # Use the shared schema mapping to get migrated_from_id (POP_ID)
            # and map PURPOSE1/VESTING to OccurrenceTenure__* fields
            canonical = schema.map_raw_row(raw)
            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)


OccurrenceTenureAdapter.PIPELINES = PIPELINES


class DummyRequest:
    def __init__(self, user):
        self.user = user


@register
class OccurrenceTenureImporter(BaseSheetImporter):
    slug = "occurrence_tenure"
    description = "Create OccurrenceTenure from spatial intersection and legacy CSV"

    def run(self, path: str, ctx: ImportContext, **options):
        start_time = timezone.now()
        logger.info(
            "OccurrenceTenureImporter (%s) started at %s (dry_run=%s)",
            self.slug,
            start_time.isoformat(),
            ctx.dry_run,
        )

        stats = ctx.stats.setdefault(self.slug, self.new_stats())

        # Get the user for versioning
        user = None
        if ctx.user_id:
            try:
                user = EmailUserRO.objects.get(id=ctx.user_id)
            except EmailUserRO.DoesNotExist:
                pass

        # If no user found, try to find a system user or similar, or just use None (might fail if save expects user)
        # populate_occurrence_tenure_data uses request.user.
        if not user:
            # Fallback to first superuser or similar if needed, but for migration usually we might have a specific user
            # For now let's assume ctx.user_id is provided or we can use a dummy user if allowed.
            # If ctx.user_id is None, we might need to fetch a default user.
            try:
                user = EmailUserRO.objects.filter(is_superuser=True).first()
            except Exception:
                pass

        request = DummyRequest(user)

        # 1. Extract
        adapter = OccurrenceTenureAdapter()
        result = adapter.extract(path, **options)
        all_rows = result.rows

        # Apply limit
        limit = getattr(ctx, "limit", None)
        if limit:
            all_rows = all_rows[: int(limit)]

        # 2. Build pipelines
        from boranga.components.data_migration.registry import (
            registry as transform_registry,
        )

        pipelines = {}
        for col, names in PIPELINES.items():
            pipelines[col] = transform_registry.build_pipeline(names)

        processed = 0
        updated = 0
        skipped = 0
        errors = 0

        # Get the tenure intersect layer
        try:
            intersect_layer = TileLayer.objects.get(
                is_tenure_intersects_query_layer=True
            )
        except TileLayer.DoesNotExist:
            logger.error("No tenure intersects query layer specified")
            return stats
        except TileLayer.MultipleObjectsReturned:
            logger.error("Multiple tenure intersects query layers found")
            return stats

        for row in all_rows:
            processed += 1
            if processed % 100 == 0:
                logger.info(f"Processed {processed} rows")

            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            transformed = {}
            has_error = False

            for col, pipeline in pipelines.items():
                raw_val = row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)
                transformed[col] = res.value
                if any(i.level == "error" for i in res.issues):
                    has_error = True
                    # Log error
                    logger.error(f"Error transforming {col}: {res.issues}")

            if has_error:
                skipped += 1
                errors += 1
                continue

            migrated_from_id = transformed.get("migrated_from_id")
            if not migrated_from_id:
                skipped += 1
                continue

            # Find Occurrence
            try:
                occurrence = Occurrence.objects.get(migrated_from_id=migrated_from_id)
            except Occurrence.DoesNotExist:
                logger.warning(
                    f"Occurrence with migrated_from_id {migrated_from_id} not found"
                )
                skipped += 1
                continue
            except Occurrence.MultipleObjectsReturned:
                logger.warning(
                    f"Multiple Occurrences with migrated_from_id {migrated_from_id} found"
                )
                skipped += 1
                continue

            # Get Geometry
            try:
                # Assuming one geometry per occurrence for now, or taking the first one
                # OccurrenceGeometry has a OneToOne or ForeignKey?
                # In utils.py: InstanceGeometry.objects.filter(**{instance_fk_field_name: instance})
                # OccurrenceGeometry is the model.
                from boranga.components.occurrence.models import OccurrenceGeometry

                geometry_instance = OccurrenceGeometry.objects.get(
                    occurrence=occurrence
                )
            except OccurrenceGeometry.DoesNotExist:
                logger.warning(f"No geometry for Occurrence {occurrence}")
                skipped += 1
                continue
            except OccurrenceGeometry.MultipleObjectsReturned:
                # If multiple, maybe process all?
                # For now let's take the first one or log warning
                logger.warning(f"Multiple geometries for Occurrence {occurrence}")
                geometry_instance = OccurrenceGeometry.objects.filter(
                    occurrence=occurrence
                ).first()

            if ctx.dry_run:
                logger.info(
                    f"Dry run: Would process tenure for Occurrence {occurrence}"
                )
                continue

            row_start_time = time.time()
            try:
                # Intersect
                # intersect_geometry_with_layer expects a GEOSGeometry
                intersect_data = intersect_geometry_with_layer(
                    geometry_instance.geometry, intersect_layer
                )

                features = intersect_data.get("features", [])
                if not features:
                    logger.info(
                        f"No intersecting tenure features for Occurrence {occurrence}"
                    )
                    continue

                # Populate Tenure
                # This creates/updates OccurrenceTenure objects
                populate_occurrence_tenure_data(geometry_instance, features, request)

                # Now update with CSV data
                purpose_id = transformed.get("OccurrenceTenure__purpose_id")
                vesting_id = transformed.get("OccurrenceTenure__vesting_id")

                # Only apply to the first tenure if multiple exist
                tenures = OccurrenceTenure.objects.filter(
                    occurrence_geometry=geometry_instance
                ).order_by("id")
                if tenures.exists():
                    tenure = tenures.first()
                    updated_fields = []
                    if purpose_id:
                        tenure.purpose_id = purpose_id
                        updated_fields.append("purpose")
                    if vesting_id:
                        tenure.vesting_id = vesting_id
                        updated_fields.append("vesting")

                    tenure.significant_to_occurrence = True
                    updated_fields.append("significant_to_occurrence")

                    tenure.save(version_user=user)
                    updated += 1

                    duration = time.time() - row_start_time
                    logger.info(
                        f"Processed Occurrence {occurrence} (Tenure updated) in {duration:.4f}s"
                    )

            except Exception as e:
                logger.exception(
                    f"Error processing tenure for Occurrence {occurrence}: {e}"
                )
                errors += 1

        stats.update(
            processed=processed,
            updated=updated,
            skipped=skipped,
            errors=errors,
        )

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)
        logger.info(f"OccurrenceTenureImporter finished: {stats}")
        return stats
