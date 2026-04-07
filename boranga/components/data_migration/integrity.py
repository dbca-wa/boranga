"""Cross-contamination detection for migration runs.

Computes per-source, per-table MD5 checksums using PostgreSQL's built-in
``md5()`` and ``string_agg()`` functions.  By snapshotting *before* and *after*
a handler runs, we can verify that data belonging to other sources was not
modified, deleted, or added.

Usage
-----
Each handler must declare an ``integrity_tables`` list on the class.  Entries
can be:

* A bare table name (str) — the table must have a ``migration_run_id`` column.
  Example: ``"boranga_species"``

* A ``(child_table, parent_table, fk_column)`` tuple — for child tables that
  lack their own ``migration_run_id`` but can be reached via a FK to a parent
  that does.
  Example: ``("boranga_occurrencedocument", "boranga_occurrence", "occurrence_id")``
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from django.db import connections

from boranga.components.data_migration.adapters.sources import SOURCE_GROUP_TYPE_MAP
from boranga.components.main.models import MigrationRun, MigrationSourceChecksum

if TYPE_CHECKING:
    from boranga.components.data_migration.registry import BaseSheetImporter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

# Direct table with migration_run_id
_CHECKSUM_DIRECT_SQL = """
SELECT
    md5(COALESCE(string_agg(md5(t.*::text), '' ORDER BY t.id), '')) AS checksum,
    count(*) AS row_count
FROM {table} t
WHERE t.migration_run_id IN (
    SELECT mr.id
    FROM boranga_migrationrun mr
    WHERE mr.options @> %s::jsonb
)
"""

# Child table joined via FK to a parent that has migration_run_id
_CHECKSUM_CHILD_SQL = """
SELECT
    md5(COALESCE(string_agg(md5(t.*::text), '' ORDER BY t.id), '')) AS checksum,
    count(*) AS row_count
FROM {child_table} t
JOIN {parent_table} p ON t.{fk_column} = p.id
WHERE p.migration_run_id IN (
    SELECT mr.id
    FROM boranga_migrationrun mr
    WHERE mr.options @> %s::jsonb
)
"""

_MD5_EMPTY = "d41d8cd98f00b204e9800998ecf8427e"  # md5('')


def _compute_checksum(table_spec: str | tuple, sources_json: str) -> tuple[str, int, str]:
    """Execute the checksum query and return (hex_digest, row_count, display_name)."""
    conn = connections["default"]
    if isinstance(table_spec, tuple):
        child_table, parent_table, fk_column = table_spec
        sql = _CHECKSUM_CHILD_SQL.format(
            child_table=child_table,
            parent_table=parent_table,
            fk_column=fk_column,
        )
        display = child_table
    else:
        sql = _CHECKSUM_DIRECT_SQL.format(table=table_spec)
        display = table_spec

    with conn.cursor() as cur:
        cur.execute(sql, [sources_json])
        row = cur.fetchone()
    checksum = row[0] or _MD5_EMPTY
    row_count = row[1] or 0
    return checksum, row_count, display


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _other_sources_with_runs(current_sources: list[str]) -> list[str]:
    """Return sources (other than *current_sources*) that have existing
    MigrationRun records.
    """
    top_level_current = set(current_sources)
    all_known = set(SOURCE_GROUP_TYPE_MAP.keys())
    candidates = sorted(all_known - top_level_current)

    if not candidates:
        return []

    conn = connections["default"]
    existing = []
    with conn.cursor() as cur:
        for src in candidates:
            cur.execute(
                "SELECT 1 FROM boranga_migrationrun WHERE options @> %s::jsonb LIMIT 1",
                ['{"sources": ["' + src + '"]}'],
            )
            if cur.fetchone():
                existing.append(src)
    return existing


def snapshot_checksums(
    handler: BaseSheetImporter,
    current_sources: list[str],
    migration_run: MigrationRun,
    phase: str,
) -> list[MigrationSourceChecksum]:
    """Compute and persist checksums for all *other* sources' data in the
    handler's target tables.

    Returns the list of created ``MigrationSourceChecksum`` rows.
    """
    tables = getattr(handler, "integrity_tables", None)
    if not tables:
        logger.debug(
            "Handler %s has no integrity_tables defined; skipping %s checksum.",
            handler.slug,
            phase,
        )
        return []

    other_sources = _other_sources_with_runs(current_sources)
    if not other_sources:
        logger.info(
            "No prior migration runs for other sources; skipping %s checksums for %s.",
            phase,
            handler.slug,
        )
        return []

    created = []
    for src in other_sources:
        sources_json = '{"sources": ["' + src + '"]}'
        for table_spec in tables:
            checksum, row_count, display = _compute_checksum(table_spec, sources_json)
            obj = MigrationSourceChecksum.objects.create(
                migration_run=migration_run,
                handler_slug=handler.slug,
                source=src,
                target_table=display,
                checksum=checksum,
                row_count=row_count,
                phase=phase,
            )
            created.append(obj)
            logger.info(
                "[%s] %s checksum for %s.%s: %s (%d rows)",
                handler.slug,
                phase.upper(),
                src,
                display,
                checksum,
                row_count,
            )
    return created


def compare_checksums(
    migration_run: MigrationRun,
    handler_slug: str,
) -> list[dict]:
    """Compare pre and post checksums for a given run/handler.

    Returns a list of dicts describing any mismatches.  An empty list means
    no cross-contamination was detected.
    """
    pre_qs = MigrationSourceChecksum.objects.filter(
        migration_run=migration_run,
        handler_slug=handler_slug,
        phase=MigrationSourceChecksum.PHASE_PRE,
    )
    post_qs = MigrationSourceChecksum.objects.filter(
        migration_run=migration_run,
        handler_slug=handler_slug,
        phase=MigrationSourceChecksum.PHASE_POST,
    )

    pre_map = {(c.source, c.target_table): c for c in pre_qs}
    post_map = {(c.source, c.target_table): c for c in post_qs}

    mismatches = []
    for key, pre in pre_map.items():
        post = post_map.get(key)
        if post is None:
            mismatches.append(
                {
                    "source": pre.source,
                    "table": pre.target_table,
                    "pre_checksum": pre.checksum,
                    "pre_row_count": pre.row_count,
                    "post_checksum": None,
                    "post_row_count": None,
                    "detail": "Post-run checksum missing.",
                }
            )
        elif pre.checksum != post.checksum or pre.row_count != post.row_count:
            mismatches.append(
                {
                    "source": pre.source,
                    "table": pre.target_table,
                    "pre_checksum": pre.checksum,
                    "pre_row_count": pre.row_count,
                    "post_checksum": post.checksum,
                    "post_row_count": post.row_count,
                    "detail": "Checksum mismatch — data was modified.",
                }
            )
    return mismatches
