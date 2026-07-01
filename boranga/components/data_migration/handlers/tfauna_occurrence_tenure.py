from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

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
class TfaunaOccurrenceTenureImporter(BaseSheetImporter):
    """
    Create OccurrenceTenure records for all TFAUNA occurrences by spatially intersecting
    each occurrence's geometry with the cadastre layer.  No purpose, vesting, or
    significant_to_occurrence values are set — all fields are left at their defaults.

    This importer is fully DB-driven: it queries existing TFAUNA Occurrences
    (migrated_from_id starting with 'tfauna-orf-') rather than reading a legacy CSV.
    Those Occurrences are auto-created by the `occurrence_report_legacy` TFAUNA run from
    approved OCRs.  The `path` argument accepted by the management command is required by
    the framework but is not used — pass any existing path (e.g. the TFAUNA legacy data
    directory) to satisfy the CLI.

    Because TFAUNA OCCs are owned by the `occurrence_report_legacy` run (not a standalone
    `occurrence_legacy` run), this handler must be run AFTER the `occurrence_report_legacy`
    TFAUNA chunks complete and the OCCs exist in the database.

    When re-running with --wipe-targets, run this handler BEFORE re-running
    `occurrence_report_legacy --sources TFAUNA --wipe-targets` so that existing tenure can
    be located via the still-present TFAUNA OCC records (Option A).

    Example:
        ./manage.py migrate_data run tfauna_occurrence_tenure \\
            private-media/legacy_data/TFAUNA/ --wipe-targets --seed-history
    """

    slug = "tfauna_occurrence_tenure"
    description = "Create OccurrenceTenure for TFAUNA occurrences via spatial intersection (DB-driven, no CSV needed)"
    integrity_tables = ["boranga_occurrence"]

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """Delete OccurrenceTenure rows for TFAUNA occurrences only.

        TFAUNA tenures can be identified via the occurrence_geometry → occurrence →
        migrated_from_id 'tfauna-orf-' prefix.  Already-historical tenures
        (occurrence_geometry set to NULL after a prior wipe) are identified via
        historical_occurrence pointing to a TFAUNA occurrence's PK.

        NOTE: This requires TFAUNA Occurrences to still exist in the database.  Always
        run this handler's --wipe-targets BEFORE running
        `occurrence_report_legacy --sources TFAUNA --wipe-targets`.
        """
        if ctx.dry_run:
            logger.info("tfauna_occurrence_tenure.clear_targets: dry-run, skipping delete")
            return

        from django.db.models import Q

        tfauna_occ_ids = list(
            Occurrence.objects.filter(migrated_from_id__startswith="tfauna-orf-").values_list("id", flat=True)
        )
        if not tfauna_occ_ids:
            logger.info("tfauna_occurrence_tenure.clear_targets: no TFAUNA occurrences found, nothing to delete")
            return

        from django.contrib.contenttypes.models import ContentType
        from reversion.models import Version

        qs = OccurrenceTenure.objects.filter(
            Q(occurrence_geometry__occurrence_id__in=tfauna_occ_ids) | Q(historical_occurrence__in=tfauna_occ_ids)
        )
        ct = ContentType.objects.get_for_model(OccurrenceTenure)
        tenure_ids = list(qs.values_list("id", flat=True))
        if tenure_ids:
            Version.objects.filter(content_type=ct, object_id__in=[str(i) for i in tenure_ids]).delete()
            deleted_count, _ = qs.delete()
            logger.info("tfauna_occurrence_tenure.clear_targets: deleted %d OccurrenceTenure rows", deleted_count)
        else:
            logger.info("tfauna_occurrence_tenure.clear_targets: no TFAUNA OccurrenceTenure rows to delete")

    def run(self, path: str, ctx: ImportContext, **options):
        # `path` is accepted for framework compatibility but is not used — all data
        # comes from the database.
        start_time = timezone.now()
        logger.info(
            "TfaunaOccurrenceTenureImporter (%s) started at %s (dry_run=%s)",
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

            tfauna_email = _SOURCE_DEFAULT_USER_MAP.get(Source.TFAUNA.value)
            if tfauna_email:
                try:
                    user = EmailUserRO.objects.get(email=tfauna_email)
                except EmailUserRO.DoesNotExist:
                    logger.warning(
                        "Migration service account '%s' for source TFAUNA not found; "
                        "OccurrenceTenure revisions will be attributed to no user.",
                        tfauna_email,
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

        # Query all TFAUNA occurrences that have a valid geometry.
        # Annotate with the PK of their latest non-null geometry to avoid N+1 fetches.
        geom_qs = (
            OccurrenceGeometry.objects.filter(occurrence=OuterRef("pk"), geometry__isnull=False)
            .order_by("-id")
            .values("pk")[:1]
        )

        qs_occs = (
            Occurrence.objects.filter(migrated_from_id__startswith="tfauna-orf-")
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

        logger.info("TfaunaOccurrenceTenureImporter: found %d TFAUNA occurrences with geometry", len(occ_list))

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

        processed = len(occ_list)
        created = 0
        updated = 0
        skipped = 0
        errors = 0
        errors_details = []
        warnings_details = []

        # Pre-deduplicate by geom_pk and filter out invalid geometries before
        # submitting to the thread pool.  Keeps worker logic simple and avoids
        # a shared mutable set inside threads.
        seen_geom_pks: set[int] = set()
        work_items: list[tuple[str, int, int]] = []
        for mid, occ_pk, geom_pk in occ_list:
            geometry_instance = geometry_map.get(geom_pk)
            if geom_pk in seen_geom_pks or geometry_instance is None or not geometry_instance.geometry:
                skipped += 1
                if geom_pk in seen_geom_pks:
                    reason = "Duplicate geometry PK"
                elif geometry_instance is None:
                    reason = "Geometry instance not found in preload map"
                else:
                    reason = "Null geometry field"
                warnings_details.append(
                    {
                        "migrated_from_id": mid,
                        "column": "geometry",
                        "level": "warning",
                        "message": f"Skipped (pre-filter): {reason}",
                        "raw_value": str(geom_pk),
                        "reason": reason,
                        "row_json": json.dumps(
                            {"migrated_from_id": mid, "occ_pk": occ_pk, "geom_pk": geom_pk}, default=str
                        ),
                        "timestamp": timezone.now().isoformat(),
                    }
                )
                continue
            if ctx.dry_run:
                logger.info("Dry run: would process tenure for TFAUNA Occurrence %s (pk=%s)", mid, occ_pk)
                continue
            seen_geom_pks.add(geom_pk)
            work_items.append((mid, occ_pk, geom_pk))

        if not ctx.dry_run:

            def _process_one(item: tuple[str, int, int]) -> dict:
                """Intersect and write OccurrenceTenure for one occurrence geometry.

                Runs in a worker thread — each thread gets its own DB connection
                from Django's thread-local connection registry.
                """
                from django.db import close_old_connections

                close_old_connections()

                mid, occ_pk, geom_pk = item
                geometry_instance = geometry_map[geom_pk]  # guaranteed valid by pre-filter

                try:
                    exists_before = not options.get("wipe_targets") and geometry_instance.id in geometry_has_tenure

                    intersect_data = intersect_geometry_with_layer(geometry_instance.geometry, intersect_layer)
                    features = intersect_data.get("features", [])

                    if not features:
                        return {"status": "skipped", "reason": "No intersection features returned from tenure layer"}

                    tenure_count_before = tenure_count_map.get(geometry_instance.id, 0)

                    # skip_revision=True: reversion history is bulk-inserted by the
                    # MigratedHistorySeeder after the run (--seed-history flag), which
                    # is far faster than one versioned save() per tenure record.
                    populate_occurrence_tenure_data(geometry_instance, features, request, skip_revision=True)

                    tenure_count_after = OccurrenceTenure.objects.filter(occurrence_geometry=geometry_instance).count()

                    if tenure_count_after == 0:
                        return {"status": "skipped", "reason": "No OccurrenceTenure records created after intersection"}

                    if not exists_before:
                        return {"status": "ok", "created": tenure_count_after, "updated": 0}
                    num_new = max(0, tenure_count_after - tenure_count_before)
                    return {"status": "ok", "created": num_new, "updated": tenure_count_before}

                except Exception as e:
                    logger.exception("Error processing tenure for TFAUNA Occurrence %s: %s", mid, e)
                    return {
                        "status": "error",
                        "error_detail": {
                            "migrated_from_id": mid,
                            "column": "general",
                            "level": "error",
                            "message": str(e),
                            "raw_value": None,
                            "reason": "Exception",
                            "row_json": json.dumps({"migrated_from_id": mid}, default=str),
                            "timestamp": timezone.now().isoformat(),
                        },
                    }

            # Number of parallel workers.  Each worker holds one DB connection.
            # 8 is the empirically determined optimum — beyond this PostGIS becomes
            # CPU-bound and throughput degrades.  Override via TFAUNA_TENURE_WORKERS
            # if the target DB has more/fewer cores available.
            num_workers = int(os.environ.get("TFAUNA_TENURE_WORKERS", "8"))
            logger.info(
                "TfaunaOccurrenceTenureImporter: processing %d occurrences with %d worker thread(s)",
                len(work_items),
                num_workers,
            )

            completed_count = 0
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_item = {executor.submit(_process_one, item): item for item in work_items}
                for future in as_completed(future_to_item):
                    completed_count += 1
                    if completed_count % 500 == 0:
                        logger.info(
                            "TfaunaOccurrenceTenureImporter: completed %d / %d",
                            completed_count,
                            len(work_items),
                        )
                    result = future.result()
                    if result["status"] == "skipped":
                        skipped += 1
                        mid, occ_pk, geom_pk = future_to_item[future]
                        warnings_details.append(
                            {
                                "migrated_from_id": mid,
                                "column": "geometry",
                                "level": "warning",
                                "message": f"Skipped: {result.get('reason', 'Unknown')}",
                                "raw_value": str(geom_pk),
                                "reason": result.get("reason", "Unknown"),
                                "row_json": json.dumps(
                                    {"migrated_from_id": mid, "occ_pk": occ_pk, "geom_pk": geom_pk}, default=str
                                ),
                                "timestamp": timezone.now().isoformat(),
                            }
                        )
                    elif result["status"] == "error":
                        errors += 1
                        errors_details.append(result["error_detail"])
                    else:
                        created += result["created"]
                        updated += result["updated"]

        # Write error/warning CSV
        if errors_details or warnings_details:
            import csv

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
                    for rec in warnings_details + errors_details:
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
                logger.info(
                    "Wrote %d warning(s) and %d error(s) to %s",
                    len(warnings_details),
                    len(errors_details),
                    csv_path,
                )
            except Exception as e:
                logger.error("Failed to write error CSV to %s: %s", csv_path, e)

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=len(warnings_details),
        )

        elapsed = timezone.now() - start_time
        stats["time_taken"] = str(elapsed)
        logger.info("TfaunaOccurrenceTenureImporter finished: %s", stats)
        return stats
