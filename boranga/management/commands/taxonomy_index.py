from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import connection, transaction


class Command(BaseCommand):
    help = "Create or drop a functional index on lower(scientific_name) for the Taxonomy table."

    # Note: This command is intended only to speed up the OccurrenceReports
    # data-migration flow which performs large case-insensitive lookups on
    # `scientific_name`. The recommended workflow is:
    #  1. Run this command with `--create` (or `--ensure`) before running the
    #     migration to create the functional index.
    #  2. Run the data-migration (occurrence reports) while the index exists.
    #  3. After migration and any verification, drop the index with `--drop`
    #     if you do not want to keep it for production queries.
    #
    # The index is created/dropped CONCURRENTLY to avoid long exclusive locks,
    # but creating it still consumes I/O and CPU while it builds. Only use the
    # `--unaccent` flag if you want to index `lower(unaccent(scientific_name))`.

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--create", action="store_true", help="Create the index if missing")
        group.add_argument("--drop", action="store_true", help="Drop the index if present")
        group.add_argument(
            "--ensure",
            action="store_true",
            help="Ensure index exists (create if missing)",
        )

        parser.add_argument(
            "--index-name",
            dest="index_name",
            default="idx_taxonomy_lower_scientific_name",
            help="Index name to create/drop (default: idx_taxonomy_lower_scientific_name)",
        )

        parser.add_argument(
            "--table",
            dest="table",
            default=None,
            help=(
                "Optional schema-qualified table name to operate on. By default the Taxonomy model's "
                "db_table is used (preferred)."
            ),
        )

        parser.add_argument(
            "--unaccent",
            action="store_true",
            help="Create/drop index on lower(unaccent(scientific_name)) instead (optional).",
        )

        parser.add_argument(
            "--yes",
            action="store_true",
            help="Answer yes to any prompt (useful for scripts).",
        )

    def _index_exists(self, short_table: str, index_name: str) -> bool:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_indexes WHERE tablename = %s AND indexname = %s",
                [short_table, index_name],
            )
            return cur.fetchone() is not None

    def _exec_sql_concurrently(self, sql: str) -> None:
        # CREATE/DROP INDEX CONCURRENTLY must not run inside a transaction block
        try:
            transaction.set_autocommit(True)
            with connection.cursor() as cur:
                cur.execute(sql)
        finally:
            try:
                transaction.set_autocommit(False)
            except Exception:
                # ignore if we cannot restore autocommit
                pass

    def handle(self, *args, **options):
        create = options.get("create")
        drop = options.get("drop")
        ensure = options.get("ensure")
        idx = options.get("index_name")
        table_opt = options.get("table")
        use_unaccent = options.get("unaccent")
        yes = options.get("yes")

        # Import Taxonomy lazily to avoid import-time side effects
        from boranga.components.species_and_communities.models import Taxonomy

        table = table_opt or Taxonomy._meta.db_table
        # short table name (no schema, no quotes) for pg_indexes lookup
        short_table = table.split(".")[-1].strip('"')

        expr = "lower(unaccent(scientific_name))" if use_unaccent else "lower(scientific_name)"

        if create or ensure:
            exists = self._index_exists(short_table, idx)
            if exists:
                self.stdout.write(self.style.SUCCESS(f"Index {idx} already exists on {table}"))
            else:
                self.stdout.write(f"About to create index {idx} on {table} using expression: {expr}")
                if not yes:
                    ans = input("Proceed? [y/N]: ")
                    if ans.lower() not in ("y", "yes"):
                        self.stdout.write(self.style.WARNING("Aborting."))
                        return
                sql = f"CREATE INDEX CONCURRENTLY {idx} ON {table} ({expr});"
                try:
                    self._exec_sql_concurrently(sql)
                    self.stdout.write(self.style.SUCCESS(f"Created index {idx} on {table}"))
                except Exception as e:
                    self.stderr.write(f"Failed to create index: {e}\n")
                    raise

        if drop:
            exists = self._index_exists(short_table, idx)
            if not exists:
                self.stdout.write(self.style.WARNING(f"Index {idx} does not exist on {table}"))
                return
            self.stdout.write(f"About to drop index {idx} on {table}")
            if not yes:
                ans = input("Proceed to drop? [y/N]: ")
                if ans.lower() not in ("y", "yes"):
                    self.stdout.write(self.style.WARNING("Aborting."))
                    return
            sql = f"DROP INDEX CONCURRENTLY IF EXISTS {idx};"
            try:
                self._exec_sql_concurrently(sql)
                self.stdout.write(self.style.SUCCESS(f"Dropped index {idx} (if it existed)"))
            except Exception as e:
                self.stderr.write(f"Failed to drop index: {e}\n")
                raise

        # If ensure was used and create path executed above, report final status
        if ensure and not create:
            exists = self._index_exists(short_table, idx)
            if exists:
                self.stdout.write(self.style.SUCCESS(f"Index {idx} exists on {table}"))
            else:
                self.stdout.write(self.style.ERROR(f"Index {idx} does NOT exist on {table}"))
