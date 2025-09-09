import argparse
import os

from django.core.management.base import BaseCommand, CommandError

from boranga.components.data_migration.registry import ImportContext, all_importers, get


class Command(BaseCommand):
    help = "Import spreadsheets (list, run one, or run multiple)"

    def add_arguments(self, parser):
        sub = parser.add_subparsers(dest="action")
        sub.required = True

        sub.add_parser("list", help="List available importers")

        p_run = sub.add_parser("run", help="Run one importer")
        p_run.add_argument("slug", help="Importer slug")
        p_run.add_argument("path", help="Spreadsheet path")
        p_run.add_argument("--dry-run", action="store_true")

        p_multi = sub.add_parser("runmany", help="Run multiple importers sequentially")
        p_multi.add_argument("path", help="Spreadsheet path (shared)")
        p_multi.add_argument("--only", nargs="+", help="Subset of slugs")
        p_multi.add_argument("--dry-run", action="store_true")

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
            ctx = ImportContext(dry_run=opts["dry_run"])
            self.stdout.write(f"== {imp_cls.slug} ==")
            opts_no_path = {k: v for k, v in opts.items() if k != "path"}
            stats = imp_cls().run(opts["path"], ctx, **opts_no_path)
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
            ctx = ImportContext(dry_run=opts["dry_run"])
            for imp_cls in importers:
                self.stdout.write(f"== {imp_cls.slug} ==")
                opts_no_path = {k: v for k, v in opts.items() if k != "path"}
                stats = imp_cls().run(opts["path"], ctx, **opts_no_path)
                self.stdout.write(f"{imp_cls.slug} stats: {stats}")
            self.stdout.write(self.style.SUCCESS(f"All done: {ctx.stats}"))
            return

        raise CommandError("Unknown action")
