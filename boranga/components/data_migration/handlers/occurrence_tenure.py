from __future__ import annotations

import json
import logging

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

            # Prepend source prefix so we can match Occurrence.migrated_from_id
            mid = canonical.get("migrated_from_id")
            if mid and not str(mid).startswith(f"{Source.TPFL.value.lower()}-"):
                canonical["migrated_from_id"] = f"{Source.TPFL.value.lower()}-{mid}"

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

    def clear_targets(
        self, ctx: ImportContext, include_children: bool = False, **options
    ):
        """Delete OccurrenceTenure target data. Respect `ctx.dry_run`."""
        if ctx.dry_run:
            logger.info("Dry run: Would delete all OccurrenceTenure objects")
            return

        # We don't delete parent Occurrences, only the Tenure links
        # OccurrenceTenure is just a linking model between OccurrenceGeometry and Tenure features
        count = OccurrenceTenure.objects.count()
        logger.info(f"Deleting {count} OccurrenceTenure objects...")
        OccurrenceTenure.objects.all().delete()
        logger.info("Deletion complete.")

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
        created = 0
        updated = 0
        skipped = 0
        errors = 0
        warnings_count = 0
        errors_details = []

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

        # Preload Occurrence map for performance (migrated_from_id -> pk)
        # Optimization: Only include active occurrences since geometries are only created for them.
        # Filter out Occurrences that don't have a geometry
        occurrence_map = {}
        # Track duplicates to mimic original behavior
        duplicate_mids = set()

        qs_occs = (
            Occurrence.objects.filter(
                migrated_from_id__isnull=False,
                processing_status=Occurrence.PROCESSING_STATUS_ACTIVE,
                occurrencegeometry__isnull=False,
                occurrencegeometry__geometry__isnull=False,
            )
            .exclude(migrated_from_id="")
            .values_list("migrated_from_id", "pk")
        )

        for mid, pk in qs_occs:
            if mid in occurrence_map:
                duplicate_mids.add(mid)
            else:
                occurrence_map[mid] = pk

        # Remove duplicates from map
        for mid in duplicate_mids:
            if mid in occurrence_map:
                del occurrence_map[mid]

        from boranga.components.occurrence.models import OccurrenceGeometry

        # Preload set of occurrence_geometry IDs that already have tenure
        # This avoids the N+1Exists query inside the loop for non-wipe runs
        geometry_has_tenure = set()
        if not options.get("wipe_targets"):
            geometry_has_tenure = set(
                OccurrenceTenure.objects.exclude(occurrence_geometry=None)
                .values_list("occurrence_geometry_id", flat=True)
                .distinct()
            )

        # Silence spatial utils logger to avoid chattiness
        logging.getLogger("boranga.components.spatial.utils").setLevel(logging.WARNING)

        for row in all_rows:
            processed += 1
            if processed % 500 == 0:
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
                    errors_details.append(
                        {
                            "migrated_from_id": row.get("migrated_from_id"),
                            "column": col,
                            "level": "error",
                            "message": str(res.issues),
                            "raw_value": raw_val,
                            "reason": "Transform error",
                            "row_json": json.dumps(row, default=str),
                            "timestamp": timezone.now().isoformat(),
                        }
                    )

            if has_error:
                skipped += 1
                errors += 1
                continue

            migrated_from_id = transformed.get("migrated_from_id")
            if not migrated_from_id:
                skipped += 1
                continue

            # Find Occurrence (Optimized)
            if migrated_from_id in duplicate_mids:
                msg = f"Multiple Occurrences with migrated_from_id {migrated_from_id} found"
                errors_details.append(
                    {
                        "migrated_from_id": migrated_from_id,
                        "column": "migrated_from_id",
                        "level": "warning",
                        "message": msg,
                        "raw_value": migrated_from_id,
                        "reason": "Duplicate occurrence",
                        "row_json": json.dumps(row, default=str),
                        "timestamp": timezone.now().isoformat(),
                    }
                )
                skipped += 1
                warnings_count += 1
                continue

            occurrence_pk = occurrence_map.get(migrated_from_id)
            if not occurrence_pk:
                msg = f"Occurrence with migrated_from_id {migrated_from_id} not found"
                errors_details.append(
                    {
                        "migrated_from_id": migrated_from_id,
                        "column": "migrated_from_id",
                        "level": "warning",
                        "message": msg,
                        "raw_value": migrated_from_id,
                        "reason": "Occurrence not found",
                        "row_json": json.dumps(row, default=str),
                        "timestamp": timezone.now().isoformat(),
                    }
                )
                skipped += 1
                warnings_count += 1
                continue

            # Use hollow object for FK lookup
            occurrence = Occurrence(pk=occurrence_pk)
            occurrence_str = f"Occurrence {occurrence_pk} ({migrated_from_id})"

            # Get Geometry
            try:
                # Assuming one geometry per occurrence for now, or taking the first one
                # OccurrenceGeometry has a OneToOne or ForeignKey?
                geometry_instance = OccurrenceGeometry.objects.get(
                    occurrence=occurrence
                )
            except OccurrenceGeometry.DoesNotExist:
                msg = f"No geometry for {occurrence_str}"
                errors_details.append(
                    {
                        "migrated_from_id": migrated_from_id,
                        "column": "geometry",
                        "level": "warning",
                        "message": msg,
                        "raw_value": None,
                        "reason": "No geometry",
                        "row_json": json.dumps(row, default=str),
                        "timestamp": timezone.now().isoformat(),
                    }
                )
                skipped += 1
                warnings_count += 1
                continue
            except OccurrenceGeometry.MultipleObjectsReturned:
                # If multiple, maybe process all?
                # For now let's take the first one or log warning
                logger.warning(f"Multiple geometries for {occurrence_str}")
                geometry_instance = OccurrenceGeometry.objects.filter(
                    occurrence=occurrence
                ).first()

            if not geometry_instance.geometry:
                msg = f"Geometry object found but geometry field is None for {occurrence_str}"
                errors_details.append(
                    {
                        "migrated_from_id": migrated_from_id,
                        "column": "geometry",
                        "level": "warning",
                        "message": msg,
                        "raw_value": None,
                        "reason": "Empty geometry",
                        "row_json": json.dumps(row, default=str),
                        "timestamp": timezone.now().isoformat(),
                    }
                )
                skipped += 1
                warnings_count += 1
                continue

            if ctx.dry_run:
                logger.info(
                    f"Dry run: Would process tenure for Occurrence {occurrence}"
                )
                continue

            try:
                # Use preloaded set to check existence without DB hit
                exists_before = (
                    options.get("wipe_targets") is not True
                    and geometry_instance.id in geometry_has_tenure
                )

                # Intersect
                # intersect_geometry_with_layer expects a GEOSGeometry
                intersect_data = intersect_geometry_with_layer(
                    geometry_instance.geometry, intersect_layer
                )

                features = intersect_data.get("features", [])
                if not features:
                    msg = f"No intersecting tenure features for {occurrence_str}"
                    errors_details.append(
                        {
                            "migrated_from_id": migrated_from_id,
                            "column": "geometry",
                            "level": "warning",
                            "message": msg,
                            "raw_value": None,
                            "reason": "No intersection",
                            "row_json": json.dumps(row, default=str),
                            "timestamp": timezone.now().isoformat(),
                        }
                    )
                    skipped += 1
                    warnings_count += 1
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

                    if not exists_before:
                        created += 1
                    else:
                        updated += 1

                    # duration = time.time() - row_start_time
                    # logger.info(
                    #     f"Processed Occurrence {occurrence} ({action_str}) in {duration:.4f}s"
                    # )

            except Exception as e:
                logger.exception(
                    f"Error processing tenure for Occurrence {occurrence}: {e}"
                )
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": migrated_from_id,
                        "column": "general",
                        "level": "error",
                        "message": str(e),
                        "raw_value": None,
                        "reason": "Exception",
                        "row_json": json.dumps(row, default=str),
                        "timestamp": timezone.now().isoformat(),
                    }
                )

        # Write error CSV
        if errors_details:
            import csv
            import os

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
                                "row_json": rec.get("row_json"),
                                "timestamp": rec.get("timestamp"),
                            }
                        )
                logger.info(f"Wrote {len(errors_details)} error details to {csv_path}")
            except Exception as e:
                logger.error(f"Failed to write error CSV to {csv_path}: {e}")

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warnings_count,
        )

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)
        logger.info(f"OccurrenceTenureImporter finished: {stats}")
        return stats
