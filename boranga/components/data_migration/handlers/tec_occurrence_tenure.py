from __future__ import annotations

import json
import logging

from django.db.models import OuterRef, Subquery
from django.utils import timezone
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    register,
)
from boranga.components.occurrence.models import Occurrence, OccurrenceGeometry, OccurrenceTenure
from boranga.components.spatial.models import TileLayer
from boranga.components.spatial.utils import (
    intersect_geometry_with_layer,
    populate_occurrence_tenure_data,
)

logger = logging.getLogger(__name__)


class DummyRequest:
    def __init__(self, user):
        self.user = user


@register
class TecOccurrenceTenureImporter(BaseSheetImporter):
    """
    Create OccurrenceTenure records for all TEC occurrences by spatially intersecting
    each occurrence's geometry with the cadastre layer.  No purpose, vesting, or
    significant_to_occurrence values are set — all fields are left at their defaults.

    Unlike the TPFL `occurrence_tenure` handler this importer is fully DB-driven:
    it queries existing TEC Occurrences (migrated_from_id starting with 'tec-') rather
    than reading a legacy CSV.  The `path` argument accepted by the management command
    is required by the framework but is not used — pass any existing path (e.g. the
    TEC legacy data directory) to satisfy the CLI.

    Example:
        ./manage.py migrate_data run tec_occurrence_tenure \\
            private-media/legacy_data/TEC/ --wipe-targets --seed-history
    """

    slug = "tec_occurrence_tenure"
    description = "Create OccurrenceTenure for TEC occurrences via spatial intersection (DB-driven, no CSV needed)"
    integrity_tables = ["boranga_occurrence"]

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """Delete OccurrenceTenure rows for TEC occurrences only.

        TEC tenures can be identified via the occurrence_geometry → occurrence →
        migrated_from_id 'tec-' prefix.  Already-historical tenures (occurrence_geometry
        set to NULL after a prior wipe) are identified via historical_occurrence pointing
        to a TEC occurrence's PK.
        """
        if ctx.dry_run:
            logger.info("tec_occurrence_tenure.clear_targets: dry-run, skipping delete")
            return

        from django.db.models import Q

        tec_occ_ids = list(Occurrence.objects.filter(migrated_from_id__startswith="tec-").values_list("id", flat=True))
        if not tec_occ_ids:
            logger.info("tec_occurrence_tenure.clear_targets: no TEC occurrences found, nothing to delete")
            return

        from django.contrib.contenttypes.models import ContentType
        from reversion.models import Version

        qs = OccurrenceTenure.objects.filter(
            Q(occurrence_geometry__occurrence_id__in=tec_occ_ids) | Q(historical_occurrence__in=tec_occ_ids)
        )
        ct = ContentType.objects.get_for_model(OccurrenceTenure)
        tenure_ids = list(qs.values_list("id", flat=True))
        if tenure_ids:
            Version.objects.filter(content_type=ct, object_id__in=[str(i) for i in tenure_ids]).delete()
            deleted_count, _ = qs.delete()
            logger.info("tec_occurrence_tenure.clear_targets: deleted %d OccurrenceTenure rows", deleted_count)
        else:
            logger.info("tec_occurrence_tenure.clear_targets: no TEC OccurrenceTenure rows to delete")

    def run(self, path: str, ctx: ImportContext, **options):
        # `path` is accepted for framework compatibility but is not used — all data
        # comes from the database.
        start_time = timezone.now()
        logger.info(
            "TecOccurrenceTenureImporter (%s) started at %s (dry_run=%s)",
            self.slug,
            start_time.isoformat(),
            ctx.dry_run,
        )

        stats = ctx.stats.setdefault(self.slug, self.new_stats())

        # Resolve user for versioning
        user = None
        if ctx.user_id:
            try:
                user = EmailUserRO.objects.get(id=ctx.user_id)
            except EmailUserRO.DoesNotExist:
                pass

        if not user:
            from boranga.components.data_migration.registry import _SOURCE_DEFAULT_USER_MAP

            tec_email = _SOURCE_DEFAULT_USER_MAP.get(Source.TEC.value)
            if tec_email:
                try:
                    user = EmailUserRO.objects.get(email=tec_email)
                except EmailUserRO.DoesNotExist:
                    logger.warning(
                        "Migration service account '%s' for source TEC not found; "
                        "OccurrenceTenure revisions will be attributed to no user.",
                        tec_email,
                    )

        request = DummyRequest(user)

        # Get the tenure intersect layer
        try:
            intersect_layer = TileLayer.objects.get(is_tenure_intersects_query_layer=True)
        except TileLayer.DoesNotExist:
            logger.error("No tenure intersects query layer specified")
            return stats
        except TileLayer.MultipleObjectsReturned:
            logger.error("Multiple tenure intersects query layers found")
            return stats

        # Query all TEC occurrences that have a valid geometry.
        # Annotate with the PK of their latest non-null geometry to avoid N+1 fetches.
        geom_qs = (
            OccurrenceGeometry.objects.filter(occurrence=OuterRef("pk"), geometry__isnull=False)
            .order_by("-id")
            .values("pk")[:1]
        )

        qs_occs = (
            Occurrence.objects.filter(migrated_from_id__startswith="tec-")
            .annotate(geom_pk=Subquery(geom_qs))
            .exclude(geom_pk=None)
            .values_list("migrated_from_id", "pk", "geom_pk")
        )

        # Apply optional limit
        limit = getattr(ctx, "limit", None)
        occ_list = list(qs_occs)
        if limit:
            try:
                occ_list = occ_list[: int(limit)]
            except Exception:
                pass

        logger.info("TecOccurrenceTenureImporter: found %d TEC occurrences with geometry", len(occ_list))

        # Preload all OccurrenceGeometry instances in one query to avoid N+1 selects in the loop.
        all_geom_pks = [geom_pk for _, _, geom_pk in occ_list]
        geometry_map: dict[int, OccurrenceGeometry] = {
            g.pk: g for g in OccurrenceGeometry.objects.filter(pk__in=all_geom_pks)
        }

        # Preload set of geometry IDs that already have tenure (for non-wipe runs)
        geometry_has_tenure: set[int] = set()
        if not options.get("wipe_targets"):
            geometry_has_tenure = set(
                OccurrenceTenure.objects.exclude(occurrence_geometry=None)
                .values_list("occurrence_geometry_id", flat=True)
                .distinct()
            )

        # Preload existing tenure counts for geometries that already have tenure.
        # This avoids a COUNT query per geometry on re-runs (wipe_targets=False).
        from django.db.models import Count

        tenure_count_map: dict[int, int] = {}
        if not options.get("wipe_targets") and geometry_has_tenure:
            geom_pks_with_tenure = {gpk for gpk in all_geom_pks if gpk in geometry_has_tenure}
            if geom_pks_with_tenure:
                tenure_count_map = dict(
                    OccurrenceTenure.objects.filter(occurrence_geometry_id__in=geom_pks_with_tenure)
                    .values("occurrence_geometry_id")
                    .annotate(cnt=Count("id"))
                    .values_list("occurrence_geometry_id", "cnt")
                )

        # Silence spatial utils logger to avoid chattiness during bulk intersections
        logging.getLogger("boranga.components.spatial.utils").setLevel(logging.WARNING)

        processed = 0
        created = 0
        updated = 0
        skipped = 0
        errors = 0
        errors_details = []
        processed_geom_pks: set[int] = set()

        for mid, occ_pk, geom_pk in occ_list:
            processed += 1
            if processed % 500 == 0:
                logger.info("TecOccurrenceTenureImporter: processed %d / %d", processed, len(occ_list))

            if geom_pk in processed_geom_pks:
                skipped += 1
                continue

            geometry_instance = geometry_map.get(geom_pk)
            if geometry_instance is None:
                skipped += 1
                continue

            if not geometry_instance.geometry:
                skipped += 1
                continue

            if ctx.dry_run:
                logger.info("Dry run: would process tenure for TEC Occurrence %s (pk=%s)", mid, occ_pk)
                continue

            processed_geom_pks.add(geom_pk)

            try:
                exists_before = not options.get("wipe_targets") and geometry_instance.id in geometry_has_tenure

                intersect_data = intersect_geometry_with_layer(geometry_instance.geometry, intersect_layer)
                features = intersect_data.get("features", [])

                if not features:
                    skipped += 1
                    continue

                tenure_count_before = tenure_count_map.get(geometry_instance.id, 0)

                populate_occurrence_tenure_data(geometry_instance, features, request, skip_revision=True)

                tenures = list(OccurrenceTenure.objects.filter(occurrence_geometry=geometry_instance).order_by("id"))
                tenure_count_after = len(tenures)

                # Update stats
                if not exists_before:
                    created += tenure_count_after
                else:
                    num_new = max(0, tenure_count_after - tenure_count_before)
                    created += num_new
                    updated += tenure_count_before

                if tenure_count_after == 0:
                    skipped += 1
                    continue

                # Save each tenure — all default values, no purpose/vesting/significant assignment
                for tenure in tenures:
                    tenure.save(version_user=user)

            except Exception as e:
                logger.exception("Error processing tenure for TEC Occurrence %s: %s", mid, e)
                errors += 1
                errors_details.append(
                    {
                        "migrated_from_id": mid,
                        "column": "general",
                        "level": "error",
                        "message": str(e),
                        "raw_value": None,
                        "reason": "Exception",
                        "row_json": json.dumps({"migrated_from_id": mid}, default=str),
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
                logger.info("Wrote %d error details to %s", len(errors_details), csv_path)
            except Exception as e:
                logger.error("Failed to write error CSV to %s: %s", csv_path, e)

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=0,
        )

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)
        logger.info("TecOccurrenceTenureImporter finished: %s", stats)
        return stats
