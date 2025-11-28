from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from boranga.components.data_migration import registry
from boranga.components.main.models import LegacyTaxonomyMapping


class Command(BaseCommand):
    help = (
        "Normalise LegacyTaxonomyMapping. Dry-run by default; use --apply to persist."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Persist normalized values back to the database",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit number of rows processed (useful for testing)",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="When --apply is set, proceed even if normalization causes conflicts",
        )

    def handle(self, *args, **options):
        apply_changes = options.get("apply", False)
        limit = options.get("limit")
        force = options.get("force", False)

        qs = LegacyTaxonomyMapping.objects.all().order_by("pk")
        total = qs.count()
        if limit:
            qs = qs[:limit]

        changes: list[tuple[int, str, str]] = []  # (pk, orig, norm)
        norm_map: dict[str, list[int]] = {}

        for row in qs:
            orig = row.legacy_canonical_name
            norm = registry._norm_legacy_canonical_name(orig)
            if norm is None:
                continue
            # record mapping
            norm_map.setdefault(norm, []).append(row.pk)
            if norm != orig:
                changes.append((row.pk, orig, norm))

        self.stdout.write(f"Total rows inspected: {total if not limit else limit}")
        self.stdout.write(f"Potential changes: {len(changes)}")

        # detect conflicts where multiple rows normalize to same key
        conflicts = {k: v for k, v in norm_map.items() if len(v) > 1}
        self.stdout.write(f"Conflicting normalized keys: {len(conflicts)}")
        if conflicts:
            # show a small sample
            self.stdout.write("Sample conflicts (normalized_key -> pks):")
            i = 0
            for k, v in conflicts.items():
                self.stdout.write(f"  {k!r} -> {v}")
                i += 1
                if i >= 10:
                    break

        # show sample changes
        if changes:
            self.stdout.write("Sample changes (pk, orig, norm):")
            for pk, orig, norm in changes[:20]:
                self.stdout.write(f"  {pk}: {orig!r} -> {norm!r}")

        if not apply_changes:
            self.stdout.write("Dry-run complete. No changes applied.")
            return

        # apply requested
        if conflicts and not force:
            self.stdout.write(
                "Normalization results in conflicting keys. Use --force to apply anyway. Aborting."
            )
            return

        # apply updates atomically
        with transaction.atomic():
            for pk, orig, norm in changes:
                LegacyTaxonomyMapping.objects.filter(pk=pk).update(
                    legacy_canonical_name=norm
                )
        self.stdout.write(f"Applied {len(changes)} changes.")
