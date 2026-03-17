"""
Migration 0637 — Transform all geometry data from WGS 84 (EPSG:4326) to the
configured DEFAULT_SRID and update the PostGIS SRID constraint on each column.

This migration is generated as part of the configurable-CRS work.  When
DEFAULT_SRID is changed in the future (e.g. from 4283 to 7844 for GDA2020),
copy this migration, update OLD_SRID / NEW_SRID, and adjust the dependency.

HOW IT WORKS
1. For each (table, column) pair that stores geometry data:
   a. UpdateGeometrySRID() to change the column's SRID constraint first
      (PostGIS rejects writes where the data SRID differs from the column SRID).
   b. ST_Transform every non-NULL geometry from OLD_SRID → NEW_SRID.
2. The subsequent auto-generated AlterField migration (from makemigrations)
   records the new field definition so Django's model state matches the DB.

IMPORTANT: This migration MUST run BEFORE the AlterField migration that
Django generates when it detects the srid kwarg has changed on the model field.
"""

from django.db import migrations

# ----- Edit these two values when performing a future CRS change -----
OLD_SRID = 4326
NEW_SRID = 4283
# ---------------------------------------------------------------------

# All (table_name, geometry_column) pairs that need transforming.
# Add new geometry tables here if they are created after this migration.
GEOMETRY_COLUMNS = [
    ("boranga_occurrencereportgeometry", "geometry"),
    ("boranga_occurrencegeometry", "geometry"),
    ("boranga_buffergeometry", "geometry"),
    ("boranga_occurrencesite", "geometry"),
    ("boranga_plausibilitygeometry", "geometry"),
    # CadastreLayer is unmanaged (managed=False), so we handle it separately
    # only if it exists.  Uncomment if you want to include it:
    # ('"public"."kb_cadastre"', "geom"),
]


def transform_forward(apps, schema_editor):
    """Transform geometry data from OLD_SRID to NEW_SRID."""
    if OLD_SRID == NEW_SRID:
        return  # Nothing to do

    with schema_editor.connection.cursor() as cursor:
        for table, column in GEOMETRY_COLUMNS:
            # Check if table exists (safety for optional/unmanaged tables)
            cursor.execute(
                "SELECT EXISTS ("
                "  SELECT FROM information_schema.tables "
                "  WHERE table_name = %s"
                ")",
                [table.strip('"').split(".")[-1].strip('"')],
            )
            if not cursor.fetchone()[0]:
                continue

            # Update the SRID constraint on the column FIRST so PostGIS
            # allows writing transformed (NEW_SRID) data into the column.
            cursor.execute(
                "SELECT UpdateGeometrySRID(%s, %s, %s)",
                [table, column, NEW_SRID],
            )

            # Transform all non-NULL geometries
            cursor.execute(
                f'UPDATE {table} SET {column} = ST_Transform({column}, %s) '
                f'WHERE {column} IS NOT NULL AND ST_SRID({column}) = %s',
                [NEW_SRID, OLD_SRID],
            )


def transform_reverse(apps, schema_editor):
    """Reverse: transform geometry data from NEW_SRID back to OLD_SRID."""
    if OLD_SRID == NEW_SRID:
        return

    with schema_editor.connection.cursor() as cursor:
        for table, column in GEOMETRY_COLUMNS:
            cursor.execute(
                "SELECT EXISTS ("
                "  SELECT FROM information_schema.tables "
                "  WHERE table_name = %s"
                ")",
                [table.strip('"').split(".")[-1].strip('"')],
            )
            if not cursor.fetchone()[0]:
                continue

            cursor.execute(
                "SELECT UpdateGeometrySRID(%s, %s, %s)",
                [table, column, OLD_SRID],
            )

            cursor.execute(
                f'UPDATE {table} SET {column} = ST_Transform({column}, %s) '
                f'WHERE {column} IS NOT NULL AND ST_SRID({column}) = %s',
                [OLD_SRID, NEW_SRID],
            )


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0636_add_last_modified_by_to_occurrence"),
    ]

    operations = [
        migrations.RunPython(
            transform_forward,
            transform_reverse,
            elidable=False,
        ),
    ]
