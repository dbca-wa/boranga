import argparse
import os

from django.core.management.base import BaseCommand, CommandError

from boranga.components.data_migration.registry import ImportContext, all_importers, get
from boranga.components.main.models import MigrationRun


class Command(BaseCommand):
    """
    Example command:
        ./manage.py migrate_data run species_legacy \
            private-media/legacy_data/TPFL/DRF_TAXON_CONSV_LISTINGS.csv --dry-run
    """

    help = "Import spreadsheets (list, run one, or run multiple)"

    def add_arguments(self, parser):
        sub = parser.add_subparsers(dest="action")
        sub.required = True

        sub.add_parser("list", help="List available importers")

        p_run = sub.add_parser("run", help="Run one importer")
        p_run.add_argument("slug", help="Importer slug")
        p_run.add_argument("path", help="Spreadsheet path")
        p_run.add_argument("--dry-run", action="store_true")
        p_run.add_argument(
            "--wipe-targets",
            action="store_true",
            help="If set, delete target model data for this importer before running (no-op in dry-run).",
        )
        p_run.add_argument(
            "--error-csv",
            type=str,
            help="Path to write error details CSV (default: auto-generated in private-media/handler_output/)",
        )
        p_run.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Optional limit of rows to process (for testing)",
        )

        p_multi = sub.add_parser("runmany", help="Run multiple importers sequentially")
        p_multi.add_argument("path", help="Spreadsheet path (shared)")
        p_multi.add_argument("--only", nargs="+", help="Subset of slugs")
        p_multi.add_argument("--dry-run", action="store_true")
        p_multi.add_argument(
            "--error-csv",
            type=str,
            help="Path to write error details CSV (default: auto-generated in private-media/handler_output/)",
        )
        p_multi.add_argument(
            "--wipe-targets",
            action="store_true",
            help="If set, delete target model data for all importers before running (no-op in dry-run).",
        )
        p_multi.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Optional limit of rows to process (for testing)",
        )

        # Manually add --sources to allow any value (avoid conflicts between importers)
        p_run.add_argument(
            "--sources",
            nargs="+",
            help="Subset of sources (e.g. TPFL, TEC)",
        )
        p_multi.add_argument(
            "--sources",
            nargs="+",
            help="Subset of sources (e.g. TPFL, TEC)",
        )

        # Importer-specific options only relevant to run / runmany
        for imp_cls in all_importers():
            imp = imp_cls()
            # Some importers declare the same option names (e.g. --sources).
            # Adding identical options to the same parser raises argparse.ArgumentError.
            # Catch and ignore those conflicts so the CLI remains usable.
            try:
                imp.add_arguments(p_run)
            except argparse.ArgumentError:
                pass
            try:
                imp.add_arguments(p_multi)
            except argparse.ArgumentError:
                pass

    def handle(self, *args, **opts):
        action = opts["action"]
        if action == "list":
            for imp in all_importers():
                self.stdout.write(f"{imp.slug:15} {imp.description}")
            return

        if action == "run":
            path = opts["path"]
            if not os.path.exists(path):
                raise CommandError(f"Path not found: {path}")
            slug = opts["slug"]
            try:
                imp_cls = get(slug)
            except KeyError as e:
                raise CommandError(str(e))
            ctx = ImportContext(dry_run=opts["dry_run"], limit=opts.get("limit"))
            # Support adapters/readers that do not accept a limit option by
            # exposing an env var fallback that `SourceAdapter.read_table` honours.
            limit_val = opts.get("limit")
            if limit_val:
                os.environ["DATA_MIGRATION_LIMIT"] = str(limit_val)
            # Create a MigrationRun record for this import (unless dry-run)
            if not ctx.dry_run:
                try:
                    run_opts = {k: v for k, v in opts.items() if k != "path"}
                    migration_run = MigrationRun.objects.create(
                        name=imp_cls.slug,
                        started_by=None,
                        options=run_opts,
                    )
                    ctx.migration_run = migration_run
                except Exception:
                    # Be conservative: do not fail the whole CLI if creating the
                    # MigrationRun record fails; log to stdout and continue without it.
                    self.stdout.write(
                        self.style.WARNING(
                            "Warning: failed to create MigrationRun record; continuing without it."
                        )
                    )
            self.stdout.write(f"== {imp_cls.slug} ==")
            # Optionally clear target data for this importer before running
            if opts.get("wipe_targets"):
                if ctx.dry_run:
                    self.stdout.write(
                        self.style.WARNING("dry-run: would wipe targets (skipped).")
                    )
                else:
                    importer = imp_cls()
                    clear_fn = getattr(importer, "clear_targets", None)
                    if callable(clear_fn):
                        self.stdout.write(
                            self.style.WARNING(
                                "Wiping target data for importer %s" % imp_cls.slug
                            )
                        )
                        try:
                            clear_fn(
                                ctx=ctx,
                                include_children=False,
                                **{k: v for k, v in opts.items() if k not in ("path",)},
                            )
                        except Exception as e:
                            raise CommandError(
                                f"Failed to wipe targets for {imp_cls.slug}: {e}"
                            )
                    else:
                        self.stdout.write(
                            self.style.WARNING(
                                f"Importer {imp_cls.slug} has no clear_targets() method; skipping wipe."
                            )
                        )

            opts_no_path = {k: v for k, v in opts.items() if k != "path"}
            stats = imp_cls().run(opts["path"], ctx, **opts_no_path)
            # Cleanup env var set for limiting rows during this run
            if limit_val and os.environ.get("DATA_MIGRATION_LIMIT") == str(limit_val):
                try:
                    del os.environ["DATA_MIGRATION_LIMIT"]
                except Exception:
                    pass
            self.stdout.write(f"{imp_cls.slug} stats: {stats}")
            self.stdout.write(self.style.SUCCESS("Done."))
            return

        if action == "runmany":
            path = opts["path"]
            if not os.path.exists(path):
                raise CommandError(f"Path not found: {path}")
            wanted = opts.get("only")
            if wanted:
                # Validate slugs early
                unknown = sorted(set(wanted) - {i.slug for i in all_importers()})
                if unknown:
                    raise CommandError(f"Unknown importer(s): {', '.join(unknown)}")
            importers = [i for i in all_importers() if not wanted or i.slug in wanted]
            ctx = ImportContext(dry_run=opts["dry_run"], limit=opts.get("limit"))
            # Expose env var fallback for adapters/readers that don't accept limit
            multi_limit = opts.get("limit")
            if multi_limit:
                os.environ["DATA_MIGRATION_LIMIT"] = str(multi_limit)
            # Create a single MigrationRun for the whole runmany operation (unless dry-run)
            if not ctx.dry_run:
                try:
                    run_opts = {k: v for k, v in opts.items() if k != "path"}
                    migration_run = MigrationRun.objects.create(
                        name="runmany",
                        started_by=None,
                        options={**run_opts, "importers": [i.slug for i in importers]},
                    )
                    ctx.migration_run = migration_run
                except Exception:
                    self.stdout.write(
                        self.style.WARNING(
                            "Warning: failed to create MigrationRun record for runmany; continuing without it."
                        )
                    )
            # If requested, wipe targets for all importers first (deletes targets and children)
            if opts.get("wipe_targets"):
                if ctx.dry_run:
                    self.stdout.write(
                        self.style.WARNING(
                            "dry-run: would wipe all importer targets (skipped)."
                        )
                    )
                else:
                    for imp_cls in importers:
                        importer = imp_cls()
                        clear_fn = getattr(importer, "clear_targets", None)
                        if callable(clear_fn):
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Wiping target data for importer {imp_cls.slug}"
                                )
                            )
                            try:
                                clear_fn(
                                    ctx=ctx,
                                    include_children=True,
                                    **{k: v for k, v in opts.items() if k != "path"},
                                )
                            except Exception as e:
                                raise CommandError(
                                    f"Failed to wipe targets for {imp_cls.slug}: {e}"
                                )
                        else:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Importer {imp_cls.slug} has no clear_targets() method; skipping wipe."
                                )
                            )

            for imp_cls in importers:
                self.stdout.write(f"== {imp_cls.slug} ==")
                opts_no_path = {k: v for k, v in opts.items() if k != "path"}
                stats = imp_cls().run(opts["path"], ctx, **opts_no_path)
                self.stdout.write(f"{imp_cls.slug} stats: {stats}")
            # Cleanup env var used for limiting rows across runmany
            if multi_limit and os.environ.get("DATA_MIGRATION_LIMIT") == str(
                multi_limit
            ):
                try:
                    del os.environ["DATA_MIGRATION_LIMIT"]
                except Exception:
                    pass
            self.stdout.write(self.style.SUCCESS(f"All done: {ctx.stats}"))
            return

        raise CommandError("Unknown action")
