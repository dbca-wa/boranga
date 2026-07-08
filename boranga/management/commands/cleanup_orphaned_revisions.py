"""
Management command to delete reversion_revision rows that have no remaining
reversion_version rows pointing to them.

These "orphaned" Revisions accumulate whenever Version rows are deleted without
their parent Revision being deleted at the same time — which is the deliberate
behaviour of ReversionHistoryCleaner._raw_delete_versions() (the NOT EXISTS
check needed to find them inline would be too slow for large tables).

This command is intended to be run as a one-off maintenance step AFTER a
wipe/reseed cycle, at a low-traffic time.

Usage
-----
  # Delete all orphaned Revisions in batches of 1 000 (default)
  ./manage.py cleanup_orphaned_revisions

  # Adjust batch size
  ./manage.py cleanup_orphaned_revisions --batch-size 5000

  # Report how many orphaned Revisions exist without deleting anything
  ./manage.py cleanup_orphaned_revisions --dry-run
"""

import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Delete reversion_revision rows that have no remaining reversion_version rows (orphaned Revisions)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            metavar="N",
            help="Number of orphaned Revisions to delete per batch (default: 1000).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Count orphaned Revisions without deleting anything.",
        )

    def handle(self, *args, **opts):
        batch_size: int = opts["batch_size"]
        dry_run: bool = opts["dry_run"]

        if dry_run:
            self._dry_run_report()
            return

        self._run_cleanup(batch_size)

    # ------------------------------------------------------------------

    def _dry_run_report(self) -> None:
        self.stdout.write(self.style.WARNING("DRY-RUN: no rows will be deleted."))
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM reversion_revision r
                WHERE NOT EXISTS (
                    SELECT 1 FROM reversion_version v WHERE v.revision_id = r.id
                )
                """
            )
            (count,) = cursor.fetchone()
        self.stdout.write(f"Orphaned reversion_revision rows: {count:,}")

    def _run_cleanup(self, batch_size: int) -> None:
        """
        Delete orphaned Revisions in keyset-paginated batches.

        Uses keyset pagination (WHERE id > last_seen_id) rather than OFFSET so
        each page is O(log n) against the PK index regardless of how far into
        the table we are.  The NOT EXISTS subquery uses the FK index on
        reversion_version.revision_id and is fast once it finds no rows.
        """
        self.stdout.write("Cleaning up orphaned reversion_revision rows …")
        total_deleted = 0
        last_id = 0
        batch_num = 0

        while True:
            # Collect a page of orphaned revision IDs starting after last_id.
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT r.id
                    FROM   reversion_revision r
                    WHERE  r.id > %s
                      AND  NOT EXISTS (
                               SELECT 1 FROM reversion_version v
                               WHERE v.revision_id = r.id
                           )
                    ORDER BY r.id
                    LIMIT  %s
                    """,
                    [last_id, batch_size],
                )
                orphan_ids = [row[0] for row in cursor.fetchall()]

            if not orphan_ids:
                break

            last_id = orphan_ids[-1]
            batch_num += 1

            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM reversion_revision WHERE id = ANY(%s)",
                    [orphan_ids],
                )
                deleted = cursor.rowcount

            total_deleted += deleted
            self.stdout.write(f"  Batch {batch_num}: deleted {deleted:,} revision(s) (last id={last_id})")
            logger.info("cleanup_orphaned_revisions: batch %d — deleted %d (last id=%d)", batch_num, deleted, last_id)

        if total_deleted:
            self.stdout.write(self.style.SUCCESS(f"Done. Total orphaned revisions deleted: {total_deleted:,}"))
        else:
            self.stdout.write(self.style.SUCCESS("Done. No orphaned revisions found."))
