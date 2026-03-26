"""
Management command to transform the kb_cadastre geometry column to the
configured DEFAULT_SRID without requiring a re-download of the source data.

Use this when import_cadastre_geojson cannot be re-run (e.g. no access to the
upstream KB feed) but the geometry data needs to match the application's
current DEFAULT_SRID after a CRS migration.
"""

import logging

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger(__name__)

# The table and column as stored in PostGIS (schema-qualified)
_TABLE = "kb_cadastre"
_SCHEMA = "public"
_COLUMN = "geom"


class Command(BaseCommand):
    help = (
        f"Transform the kb_cadastre geometry column to the application's "
        f"DEFAULT_SRID (currently {settings.DEFAULT_SRID}) without re-downloading "
        f"source data. Detects the current column SRID automatically."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-srid",
            type=int,
            default=4283,
            help=(
                "The CRS the geometries are actually stored in (default: 4283 / GDA94). "
                "This is used as the source for ST_Transform and cannot be inferred "
                "reliably from the PostGIS metadata after a prior UpdateGeometrySRID call."
            ),
        )
        parser.add_argument(
            "--target-srid",
            type=int,
            default=settings.DEFAULT_SRID,
            help=f"Target SRID to transform to (default: {settings.DEFAULT_SRID})",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would be done without making any changes.",
        )

    def handle(self, *args, **options):
        source_srid = options["source_srid"]
        target_srid = options["target_srid"]
        dry_run = options["dry_run"]

        if source_srid == target_srid:
            self.stdout.write(self.style.SUCCESS("Source and target SRIDs are identical. Nothing to do."))
            return

        with connection.cursor() as cursor:
            # Check the table exists
            cursor.execute(
                "SELECT EXISTS (  SELECT FROM information_schema.tables   WHERE table_schema = %s AND table_name = %s)",
                [_SCHEMA, _TABLE],
            )
            if not cursor.fetchone()[0]:
                raise CommandError(
                    f'Table "{_SCHEMA}"."{_TABLE}" does not exist. Run import_cadastre_geojson first to populate it.'
                )

            # Verify the geometry column exists, but do NOT rely on the reported
            # SRID as the true coordinate CRS — UpdateGeometrySRID() updates that
            # metadata independently of the actual coordinate values, so it can
            # be stale or wrong after a previous failed/partial migration.
            cursor.execute(
                "SELECT srid FROM geometry_columns "
                "WHERE f_table_schema = %s AND f_table_name = %s AND f_geometry_column = %s",
                [_SCHEMA, _TABLE, _COLUMN],
            )
            if not cursor.fetchone():
                raise CommandError(
                    f'Column "{_COLUMN}" not found in geometry_columns for '
                    f'"{_SCHEMA}"."{_TABLE}". The table may not be a PostGIS geometry table.'
                )

            # Count rows to give the user an idea of how long this will take
            cursor.execute(f'SELECT COUNT(*) FROM "{_SCHEMA}"."{_TABLE}" WHERE {_COLUMN} IS NOT NULL')
            row_count = cursor.fetchone()[0]

            self.stdout.write(
                f'Transforming "{_SCHEMA}"."{_TABLE}".{_COLUMN}: '
                f"EPSG:{source_srid} → EPSG:{target_srid} "
                f"({row_count:,} non-null rows)"
            )

            if dry_run:
                self.stdout.write(self.style.WARNING("Dry run — no changes made."))
                return

            # 1. Transform all non-NULL geometries in-place.
            #
            # NOTE: We intentionally do NOT call UpdateGeometrySRID() before
            # this UPDATE.  In PostGIS, UpdateGeometrySRID() relabels the SRID
            # stored in every geometry binary (via ST_SetSRID) in addition to
            # updating the type constraint.  If we called it first, all rows
            # would immediately report ST_SRID = target_srid, making the
            # subsequent ST_Transform a no-op (0 rows matched).
            #
            # Instead we use ST_SetSRID(geom, source_srid) inside the UPDATE
            # to _force_ the declared source CRS regardless of whatever SRID is
            # currently embedded in the binary (handles SRID=0 from ogr2ogr,
            # or geometries that were previously mislabelled by an earlier
            # UpdateGeometrySRID call).  No WHERE guard is used — we always
            # apply the transform unconditionally because the embedded SRID
            # cannot be trusted after a prior UpdateGeometrySRID call.
            self.stdout.write("  Running ST_Transform (this may take a while for large datasets)...")
            cursor.execute(
                f'UPDATE "{_SCHEMA}"."{_TABLE}" '
                f"SET {_COLUMN} = ST_Transform(ST_SetSRID({_COLUMN}, %s), %s) "
                f"WHERE {_COLUMN} IS NOT NULL",
                [source_srid, target_srid],
            )
            updated = cursor.rowcount

            # 2. Update the column type constraint AFTER the data is correct.
            self.stdout.write(f"  Updating geometry column SRID constraint to {target_srid}...")
            cursor.execute(
                "SELECT UpdateGeometrySRID(%s, %s, %s, %s)",
                [_SCHEMA, _TABLE, _COLUMN, target_srid],
            )

        self.stdout.write(
            self.style.SUCCESS(f"Done. {updated:,} rows transformed from EPSG:{source_srid} to EPSG:{target_srid}.")
        )
