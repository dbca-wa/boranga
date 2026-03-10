"""
Management command to seed initial django-reversion history for all migrated records.

Usage
-----
  # Seed everything
  ./manage.py create_migrated_history

  # Seed only specific domains
  ./manage.py create_migrated_history --only species communities

  # Dry-run (print what would be seeded, create nothing)
  ./manage.py create_migrated_history --dry-run

  # Adjust batch size (default 500)
  ./manage.py create_migrated_history --batch-size 1000

Available domain names
  species
  communities
  conservation_statuses
  occurrence_reports
  occurrences
"""

import logging

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

DOMAIN_CHOICES = [
    "species",
    "communities",
    "conservation_statuses",
    "occurrence_reports",
    "occurrences",
]


class Command(BaseCommand):
    help = "Seed initial django-reversion history for all migrated records that have none."

    def add_arguments(self, parser):
        parser.add_argument(
            "--only",
            nargs="+",
            metavar="DOMAIN",
            choices=DOMAIN_CHOICES,
            help=(f"Seed only the specified domain(s). Choices: {', '.join(DOMAIN_CHOICES)}. Omit to seed everything."),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many records would be seeded without writing anything.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            metavar="N",
            help="Number of objects processed per database batch (default: 500).",
        )

    def handle(self, *args, **opts):
        from boranga.components.data_migration.history_seeding.reversion_seeder import (
            MigratedHistorySeeder,
        )

        dry_run: bool = opts["dry_run"]
        batch_size: int = opts["batch_size"]
        only: list[str] | None = opts.get("only")

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY-RUN mode: no history records will be written."))
            self._dry_run_report(only or DOMAIN_CHOICES)
            return

        seeder = MigratedHistorySeeder(batch_size=batch_size)
        domains = only or DOMAIN_CHOICES

        for domain in domains:
            self.stdout.write(f"Seeding history for: {domain} …")
            method = getattr(seeder, f"seed_{domain}")
            count = method()
            self.stdout.write(self.style.SUCCESS(f"  → {count} version(s) created"))

        stats = seeder.get_stats()
        total = sum(stats.values())
        self.stdout.write(self.style.SUCCESS(f"\nDone. Total versions created: {total}"))
        self.stdout.write(f"Breakdown: {stats}")

    def _dry_run_report(self, domains: list[str]) -> None:
        """Report counts of how many migrated objects exist per domain without writing."""
        from boranga.components.conservation_status.models import (
            ConservationStatus,
            ConservationStatusDocument,
        )
        from boranga.components.occurrence.models import (
            OCCConservationThreat,
            OCCContactDetail,
            Occurrence,
            OccurrenceDocument,
            OccurrenceReport,
            OccurrenceReportDocument,
            OccurrenceSite,
            OccurrenceTenure,
            OCRConservationThreat,
            OCRObserverDetail,
        )
        from boranga.components.species_and_communities.models import (
            Community,
            CommunityDocument,
            ConservationThreat,
            Species,
            SpeciesDocument,
        )

        domain_report: dict[str, list[tuple[str, int]]] = {
            "species": [
                ("Species", Species.objects.filter(migrated_from_id__gt="").count()),
                ("SpeciesDocument", SpeciesDocument.objects.filter(species__migrated_from_id__gt="").count()),
                (
                    "ConservationThreat(species)",
                    ConservationThreat.objects.filter(species__migrated_from_id__gt="", species__isnull=False).count(),
                ),
            ],
            "communities": [
                ("Community", Community.objects.filter(migrated_from_id__gt="").count()),
                ("CommunityDocument", CommunityDocument.objects.filter(community__migrated_from_id__gt="").count()),
                (
                    "ConservationThreat(community)",
                    ConservationThreat.objects.filter(
                        community__migrated_from_id__gt="", community__isnull=False
                    ).count(),
                ),
            ],
            "conservation_statuses": [
                ("ConservationStatus", ConservationStatus.objects.filter(migrated_from_id__gt="").count()),
                (
                    "ConservationStatusDocument",
                    ConservationStatusDocument.objects.filter(conservation_status__migrated_from_id__gt="").count(),
                ),
            ],
            "occurrence_reports": [
                (
                    "OccurrenceReport",
                    OccurrenceReport.objects.filter(migrated_from_id__isnull=False)
                    .exclude(migrated_from_id="")
                    .count(),
                ),
                (
                    "OccurrenceReportDocument",
                    OccurrenceReportDocument.objects.filter(occurrence_report__migrated_from_id__isnull=False)
                    .exclude(occurrence_report__migrated_from_id="")
                    .count(),
                ),
                (
                    "OCRConservationThreat",
                    OCRConservationThreat.objects.filter(occurrence_report__migrated_from_id__isnull=False)
                    .exclude(occurrence_report__migrated_from_id="")
                    .count(),
                ),
                (
                    "OCRObserverDetail",
                    OCRObserverDetail.objects.filter(occurrence_report__migrated_from_id__isnull=False)
                    .exclude(occurrence_report__migrated_from_id="")
                    .count(),
                ),
            ],
            "occurrences": [
                (
                    "Occurrence",
                    Occurrence.objects.filter(migrated_from_id__isnull=False).exclude(migrated_from_id="").count(),
                ),
                (
                    "OccurrenceDocument",
                    OccurrenceDocument.objects.filter(occurrence__migrated_from_id__isnull=False)
                    .exclude(occurrence__migrated_from_id="")
                    .count(),
                ),
                (
                    "OCCConservationThreat",
                    OCCConservationThreat.objects.filter(occurrence__migrated_from_id__isnull=False)
                    .exclude(occurrence__migrated_from_id="")
                    .count(),
                ),
                (
                    "OCCContactDetail",
                    OCCContactDetail.objects.filter(occurrence__migrated_from_id__isnull=False)
                    .exclude(occurrence__migrated_from_id="")
                    .count(),
                ),
                (
                    "OccurrenceSite",
                    OccurrenceSite.objects.filter(occurrence__migrated_from_id__isnull=False)
                    .exclude(occurrence__migrated_from_id="")
                    .count(),
                ),
                (
                    "OccurrenceTenure",
                    OccurrenceTenure.objects.filter(occurrence_geometry__occurrence__migrated_from_id__isnull=False)
                    .exclude(occurrence_geometry__occurrence__migrated_from_id="")
                    .count(),
                ),
            ],
        }

        total = 0
        for domain in domains:
            self.stdout.write(f"\n{domain}:")
            for model_name, count in domain_report.get(domain, []):
                self.stdout.write(f"  {model_name:<35} {count:>8} migrated object(s)")
                total += count
        self.stdout.write(f"\nTotal migrated objects that would be seeded: {total}")
