from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection, transaction


class Command(BaseCommand):
    """
    Populate OccurrenceTenure.owner_name and owner_count from the kb_cadastre layer.

    Default strategy (fast):
        Join on cad_pin — a single set-based SQL UPDATE. All records are expected
        to have a cad_pin, so this covers the vast majority of rows instantly.

    Optional spatial strategy (slow):
        Enabled via --spatial. Runs ST_Intersects against kb_cadastre.geom in
        batches for rows that still lack an owner_name after the cad_pin step.
        Use --sample / --limit / --start-id to restrict the spatial run for testing.

    Examples:
        # Dry-run (no changes):
        ./manage.py populate_tenure_owner_names --dry-run

        # Live run (cad_pin only):
        ./manage.py populate_tenure_owner_names

        # Include spatial fallback for rows still missing owner_name:
        ./manage.py populate_tenure_owner_names --spatial

        # Test spatial on 50 random rows without committing:
        ./manage.py populate_tenure_owner_names --spatial --sample 50 --dry-run
    """

    help = "Populate OccurrenceTenure.owner_name / owner_count from kb_cadastre"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help="Report how many rows would change without persisting anything.",
        )
        parser.add_argument(
            "--spatial",
            action="store_true",
            dest="spatial",
            help="After the cad_pin step, run a spatial ST_Intersects step for rows still missing owner_name.",
        )
        # Spatial-step controls
        parser.add_argument(
            "--batch-size",
            type=int,
            dest="batch_size",
            default=500,
            metavar="N",
            help="Rows per batch for the spatial step (default: 500).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            dest="limit",
            default=None,
            metavar="N",
            help="Maximum candidate rows for the spatial step (useful for quick tests).",
        )
        parser.add_argument(
            "--start-id",
            type=int,
            dest="start_id",
            default=0,
            metavar="ID",
            help="Start the spatial step from this tenure id (useful for resuming).",
        )
        parser.add_argument(
            "--sample",
            type=int,
            dest="sample",
            default=None,
            metavar="N",
            help="Run the spatial step on a random sample of N rows instead of sequentially.",
        )

    # ------------------------------------------------------------------
    # SQL
    # ------------------------------------------------------------------

    _SELECT_PIN_COUNT = """
        SELECT count(*)
        FROM boranga_occurrencetenure ot
        JOIN public.kb_cadastre c ON c.cad_pin::text = ot.cad_pin::text
        WHERE (ot.owner_name IS NULL OR ot.owner_name = '')
          AND ot.cad_pin IS NOT NULL
    """

    _UPDATE_BY_PIN = """
        UPDATE boranga_occurrencetenure ot
        SET owner_name  = c.cad_owner_name,
            owner_count = c.cad_owner_count
        FROM public.kb_cadastre c
        WHERE (ot.owner_name IS NULL OR ot.owner_name = '')
          AND ot.cad_pin IS NOT NULL
          AND c.cad_pin::text = ot.cad_pin::text
    """

    _SELECT_SPATIAL_SAMPLE = """
        SELECT id FROM boranga_occurrencetenure
        WHERE (owner_name IS NULL OR owner_name = '')
          AND tenure_area_ewkb IS NOT NULL
        ORDER BY random() LIMIT %s
    """

    _SELECT_SPATIAL_COUNT = """
        SELECT count(*) FROM boranga_occurrencetenure
        WHERE (owner_name IS NULL OR owner_name = '')
          AND tenure_area_ewkb IS NOT NULL
          AND id >= %s
    """

    _SELECT_SPATIAL_BATCH_IDS = """
        SELECT id FROM boranga_occurrencetenure
        WHERE (owner_name IS NULL OR owner_name = '')
          AND tenure_area_ewkb IS NOT NULL
          AND id > %s
        ORDER BY id LIMIT %s
    """

    _SELECT_SPATIAL_MATCH_COUNT = """
        SELECT count(*)
        FROM boranga_occurrencetenure ot
        JOIN public.kb_cadastre c
          ON ST_Intersects(ST_SetSRID(ST_GeomFromEWKB(ot.tenure_area_ewkb), %s), c.geom)
        WHERE ot.id = ANY(%s)
    """

    _UPDATE_SPATIAL_BATCH = """
        UPDATE boranga_occurrencetenure ot
        SET owner_name  = c.cad_owner_name,
            owner_count = c.cad_owner_count,
            cad_pin     = c.cad_pin
        FROM public.kb_cadastre c
        WHERE ot.id = ANY(%s)
          AND ST_Intersects(ST_SetSRID(ST_GeomFromEWKB(ot.tenure_area_ewkb), %s), c.geom)
    """

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        with connection.cursor() as cursor:
            self._run_pin_step(cursor, dry_run)

            if options["spatial"]:
                self._run_spatial_step(cursor, dry_run, options)
            else:
                self.stdout.write("Spatial step skipped (pass --spatial to enable).")

        self.stdout.write(self.style.SUCCESS("Done."))

    # ------------------------------------------------------------------
    # Steps
    # ------------------------------------------------------------------

    def _run_pin_step(self, cursor, dry_run):
        """Fast set-based UPDATE via cad_pin equality join."""
        if dry_run:
            cursor.execute(self._SELECT_PIN_COUNT)
            self.stdout.write(f"[dry-run] cad_pin: {cursor.fetchone()[0]} rows would be updated")
        else:
            with transaction.atomic():
                cursor.execute(self._UPDATE_BY_PIN)
                self.stdout.write(f"cad_pin: {cursor.rowcount} rows updated")

    def _run_spatial_step(self, cursor, dry_run, options):
        """Batched ST_Intersects UPDATE for rows still missing owner_name."""
        batch_size = options["batch_size"] or 500
        limit = options["limit"]
        start_id = options["start_id"] or 0
        sample = options["sample"]
        srid = settings.DEFAULT_SRID

        if sample:
            cursor.execute(self._SELECT_SPATIAL_SAMPLE, [sample])
            candidate_ids = [r[0] for r in cursor.fetchall()]
            total = len(candidate_ids)
            self.stdout.write(f"Spatial step: {total} random candidates")
        else:
            cursor.execute(self._SELECT_SPATIAL_COUNT, [start_id])
            total = cursor.fetchone()[0]
            if limit:
                total = min(total, limit)
            self.stdout.write(f"Spatial step: {total} candidates (start_id={start_id})")
            candidate_ids = None

        processed = 0
        updated_total = 0

        if candidate_ids is not None:
            for i in range(0, len(candidate_ids), batch_size):
                chunk = candidate_ids[i : i + batch_size]
                processed, updated_total = self._process_batch(
                    cursor, chunk, srid, dry_run, processed, updated_total, total
                )
                if limit and processed >= limit:
                    break
        else:
            last_id = max(start_id - 1, 0)
            while True:
                this_batch = min(batch_size, limit - processed) if limit else batch_size
                if this_batch <= 0:
                    break
                cursor.execute(self._SELECT_SPATIAL_BATCH_IDS, [last_id, this_batch])
                rows = cursor.fetchall()
                if not rows:
                    break
                ids = [r[0] for r in rows]
                last_id = ids[-1]
                processed, updated_total = self._process_batch(
                    cursor, ids, srid, dry_run, processed, updated_total, total
                )

        self.stdout.write(f"Spatial step complete: {processed} processed, {updated_total} updated")

    def _process_batch(self, cursor, ids, srid, dry_run, processed, updated_total, total):
        processed += len(ids)
        if dry_run:
            cursor.execute(self._SELECT_SPATIAL_MATCH_COUNT, [srid, ids])
            matched = cursor.fetchone()[0]
            self.stdout.write(f"  [dry-run] {processed}/{total} — {matched} matched in batch")
        else:
            with transaction.atomic():
                cursor.execute(self._UPDATE_SPATIAL_BATCH, [ids, srid])
                updated = cursor.rowcount
                updated_total += updated
                self.stdout.write(f"  {processed}/{total} — {updated} updated in batch")
        return processed, updated_total
