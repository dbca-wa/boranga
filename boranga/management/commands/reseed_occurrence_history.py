"""
Management command to wipe and/or reseed django-reversion history for
OccurrenceReport and Occurrence records filtered by group type.

This lets you fix corrupted / stale reversion history for a single group type
(e.g. fauna) without having to rerun the full 6+ hour data migration.

Usage
-----
  # Wipe + reseed fauna Occurrences and OccurrenceReports (and all children)
  ./manage.py reseed_occurrence_history --group-type fauna

  # Multiple group types at once
  ./manage.py reseed_occurrence_history --group-type fauna flora

  # Only OccurrenceReports, not Occurrences
  ./manage.py reseed_occurrence_history --group-type fauna --domains occurrence_reports

  # Only Occurrences, not OccurrenceReports
  ./manage.py reseed_occurrence_history --group-type fauna --domains occurrences

  # Skip the wipe step (only seed records that have no history yet)
  ./manage.py reseed_occurrence_history --group-type fauna --skip-wipe

  # Only wipe, do not reseed
  ./manage.py reseed_occurrence_history --group-type fauna --wipe-only

  # Dry-run: report counts without modifying any data
  ./manage.py reseed_occurrence_history --group-type fauna --dry-run

  # Adjust batch size (default 500 for seeding, 2000 for wiping)
  ./manage.py reseed_occurrence_history --group-type fauna --batch-size 1000

Available group-type names
  fauna
  flora
  community

Available domain names
  occurrence_reports
  occurrences
"""

import logging

from django.core.management.base import BaseCommand, CommandError

logger = logging.getLogger(__name__)

DOMAIN_CHOICES = ["occurrence_reports", "occurrences"]
GROUP_TYPE_CHOICES = ["fauna", "flora", "community"]


class Command(BaseCommand):
    help = (
        "Wipe and/or reseed django-reversion history for OccurrenceReport / Occurrence "
        "records filtered by group type, without rerunning the full data migration."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--group-type",
            nargs="+",
            metavar="GROUP_TYPE",
            choices=GROUP_TYPE_CHOICES,
            required=True,
            help=(f"One or more group types to process. Choices: {', '.join(GROUP_TYPE_CHOICES)}."),
        )
        parser.add_argument(
            "--domains",
            nargs="+",
            metavar="DOMAIN",
            choices=DOMAIN_CHOICES,
            default=DOMAIN_CHOICES,
            help=(f"Domains to process. Choices: {', '.join(DOMAIN_CHOICES)}. Default: both."),
        )
        parser.add_argument(
            "--skip-wipe",
            action="store_true",
            help=(
                "Skip deleting existing history before reseeding. "
                "Existing history will be preserved and only records with no history "
                "will receive a new baseline revision."
            ),
        )
        parser.add_argument(
            "--wipe-only",
            action="store_true",
            help="Delete history but do not reseed. Mutually exclusive with --skip-wipe.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report counts of records that would be affected without modifying anything.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            metavar="N",
            help="Batch size for database operations (default: 500 for seeding, 2000 for wiping).",
        )

    def handle(self, *args, **opts):
        group_type_names: list[str] = opts["group_type"]
        domains: list[str] = opts["domains"]
        skip_wipe: bool = opts["skip_wipe"]
        wipe_only: bool = opts["wipe_only"]
        dry_run: bool = opts["dry_run"]
        batch_size: int = opts["batch_size"]

        if skip_wipe and wipe_only:
            raise CommandError("--skip-wipe and --wipe-only are mutually exclusive.")

        group_type_filter = {"group_type__name__in": group_type_names}
        group_type_label = ", ".join(group_type_names)

        self.stdout.write(
            self.style.MIGRATE_HEADING(
                f"Occurrence history reseed — group type(s): {group_type_label} — domain(s): {', '.join(domains)}"
            )
        )

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY-RUN mode: no history records will be modified."))
            self._dry_run_report(group_type_names, domains)
            return

        # ----------------------------------------------------------------
        # Wipe phase
        # ----------------------------------------------------------------
        if not skip_wipe:
            self._run_wipe(group_type_filter, group_type_label, domains, batch_size)

        # ----------------------------------------------------------------
        # Reseed phase
        # ----------------------------------------------------------------
        if not wipe_only:
            self._run_seed(group_type_names, group_type_label, domains, batch_size)

    # ------------------------------------------------------------------
    # Wipe
    # ------------------------------------------------------------------

    def _run_wipe(
        self,
        group_type_filter: dict,
        group_type_label: str,
        domains: list[str],
        batch_size: int,
    ) -> None:
        from boranga.components.data_migration.history_cleanup.reversion_cleanup import (
            ReversionHistoryCleaner,
        )

        self.stdout.write(f"\n--- Wiping reversion history for: {group_type_label} ---")
        # Use a larger batch size for deletion (2000 is the cleaner's default,
        # but respect --batch-size if the caller specified something larger).
        wipe_batch_size = max(batch_size, 2000)
        cleaner = ReversionHistoryCleaner(batch_size=wipe_batch_size)

        if "occurrence_reports" in domains:
            self.stdout.write("  Clearing OccurrenceReport history …")
            cleaner.clear_occurrence_report_and_related(group_type_filter)

        if "occurrences" in domains:
            self.stdout.write("  Clearing Occurrence history …")
            cleaner.clear_occurrence_and_related(group_type_filter)

        wipe_stats = cleaner.get_stats()
        total_wiped = sum(wipe_stats.values())
        self.stdout.write(self.style.SUCCESS(f"  Wipe complete. Total versions deleted: {total_wiped}"))
        self.stdout.write(f"  Breakdown: {wipe_stats}")

    # ------------------------------------------------------------------
    # Seed
    # ------------------------------------------------------------------

    def _run_seed(
        self,
        group_type_names: list[str],
        group_type_label: str,
        domains: list[str],
        batch_size: int,
    ) -> None:
        from boranga.components.data_migration.history_seeding.reversion_seeder import (
            MigratedHistorySeeder,
        )

        self.stdout.write(f"\n--- Reseeding reversion history for: {group_type_label} ---")
        seeder = MigratedHistorySeeder(batch_size=batch_size)

        if "occurrence_reports" in domains:
            self.stdout.write("  Seeding OccurrenceReport history …")
            count = seeder.seed_occurrence_reports(group_type_names=group_type_names)
            self.stdout.write(self.style.SUCCESS(f"    → {count} version(s) created"))

        if "occurrences" in domains:
            self.stdout.write("  Seeding Occurrence history …")
            count = seeder.seed_occurrences(group_type_names=group_type_names)
            self.stdout.write(self.style.SUCCESS(f"    → {count} version(s) created"))

        seed_stats = seeder.get_stats()
        total_seeded = sum(seed_stats.values())
        self.stdout.write(self.style.SUCCESS(f"\nReseed complete. Total versions created: {total_seeded}"))
        self.stdout.write(f"Breakdown: {seed_stats}")

    # ------------------------------------------------------------------
    # Dry-run reporting
    # ------------------------------------------------------------------

    def _dry_run_report(self, group_type_names: list[str], domains: list[str]) -> None:
        from django.contrib.contenttypes.models import ContentType
        from reversion.models import Version

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

        gt_filter = {"group_type__name__in": group_type_names}

        rows: list[tuple[str, str, int, int]] = []  # (domain, model, total, versioned)

        def _count_versioned(model_class, qs):
            ct = ContentType.objects.get_for_model(model_class)
            pks = list(qs.values_list("pk", flat=True))
            if not pks:
                return 0
            return (
                Version.objects.filter(content_type=ct, object_id__in=[str(p) for p in pks])
                .values("object_id")
                .distinct()
                .count()
            )

        if "occurrence_reports" in domains:
            ocr_qs = (
                OccurrenceReport.objects.filter(migrated_from_id__isnull=False)
                .exclude(migrated_from_id="")
                .filter(**gt_filter)
            )
            rows.append(
                ("occurrence_reports", "OccurrenceReport", ocr_qs.count(), _count_versioned(OccurrenceReport, ocr_qs))
            )

            doc_qs = (
                OccurrenceReportDocument.objects.filter(occurrence_report__migrated_from_id__isnull=False)
                .exclude(occurrence_report__migrated_from_id="")
                .filter(occurrence_report__group_type__name__in=group_type_names)
            )
            rows.append(
                (
                    "occurrence_reports",
                    "OccurrenceReportDocument",
                    doc_qs.count(),
                    _count_versioned(OccurrenceReportDocument, doc_qs),
                )
            )

            threat_qs = (
                OCRConservationThreat.objects.filter(occurrence_report__migrated_from_id__isnull=False)
                .exclude(occurrence_report__migrated_from_id="")
                .filter(occurrence_report__group_type__name__in=group_type_names)
            )
            rows.append(
                (
                    "occurrence_reports",
                    "OCRConservationThreat",
                    threat_qs.count(),
                    _count_versioned(OCRConservationThreat, threat_qs),
                )
            )

            obs_qs = (
                OCRObserverDetail.objects.filter(occurrence_report__migrated_from_id__isnull=False)
                .exclude(occurrence_report__migrated_from_id="")
                .filter(occurrence_report__group_type__name__in=group_type_names)
            )
            rows.append(
                ("occurrence_reports", "OCRObserverDetail", obs_qs.count(), _count_versioned(OCRObserverDetail, obs_qs))
            )

        if "occurrences" in domains:
            occ_qs = (
                Occurrence.objects.filter(migrated_from_id__isnull=False)
                .exclude(migrated_from_id="")
                .filter(**gt_filter)
            )
            rows.append(("occurrences", "Occurrence", occ_qs.count(), _count_versioned(Occurrence, occ_qs)))

            doc_qs = (
                OccurrenceDocument.objects.filter(occurrence__migrated_from_id__isnull=False)
                .exclude(occurrence__migrated_from_id="")
                .filter(occurrence__group_type__name__in=group_type_names)
            )
            rows.append(
                ("occurrences", "OccurrenceDocument", doc_qs.count(), _count_versioned(OccurrenceDocument, doc_qs))
            )

            threat_qs = (
                OCCConservationThreat.objects.filter(occurrence__migrated_from_id__isnull=False)
                .exclude(occurrence__migrated_from_id="")
                .filter(occurrence__group_type__name__in=group_type_names)
            )
            rows.append(
                (
                    "occurrences",
                    "OCCConservationThreat",
                    threat_qs.count(),
                    _count_versioned(OCCConservationThreat, threat_qs),
                )
            )

            contact_qs = (
                OCCContactDetail.objects.filter(occurrence__migrated_from_id__isnull=False)
                .exclude(occurrence__migrated_from_id="")
                .filter(occurrence__group_type__name__in=group_type_names)
            )
            rows.append(
                ("occurrences", "OCCContactDetail", contact_qs.count(), _count_versioned(OCCContactDetail, contact_qs))
            )

            site_qs = (
                OccurrenceSite.objects.filter(occurrence__migrated_from_id__isnull=False)
                .exclude(occurrence__migrated_from_id="")
                .filter(occurrence__group_type__name__in=group_type_names)
            )
            rows.append(("occurrences", "OccurrenceSite", site_qs.count(), _count_versioned(OccurrenceSite, site_qs)))

            tenure_qs = (
                OccurrenceTenure.objects.filter(occurrence_geometry__occurrence__migrated_from_id__isnull=False)
                .exclude(occurrence_geometry__occurrence__migrated_from_id="")
                .filter(occurrence_geometry__occurrence__group_type__name__in=group_type_names)
            )
            rows.append(
                ("occurrences", "OccurrenceTenure", tenure_qs.count(), _count_versioned(OccurrenceTenure, tenure_qs))
            )

        self.stdout.write(f"\n{'Domain':<22} {'Model':<30} {'Records':>10} {'Versioned':>10} {'To seed':>10}")
        self.stdout.write("-" * 80)
        total_records = total_versioned = 0
        for domain, model, records, versioned in rows:
            to_seed = records - versioned
            self.stdout.write(f"{domain:<22} {model:<30} {records:>10} {versioned:>10} {to_seed:>10}")
            total_records += records
            total_versioned += versioned
        self.stdout.write("-" * 80)
        self.stdout.write(
            f"{'TOTAL':<22} {'':<30} {total_records:>10} {total_versioned:>10} {total_records - total_versioned:>10}"
        )
