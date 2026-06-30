from django.core.management.base import BaseCommand
from django.db import transaction

from boranga.components.occurrence.models import Occurrence, OccurrenceReport
from boranga.components.occurrence.utils import (
    _OCR_OCC_CHILD_RELATIONS,
    _RELATION_GROUP_EXCLUSIONS,
    fix_missing_occurrence_relations,
)


class Command(BaseCommand):
    help = (
        "Creates empty one-to-one child relations for OccurrenceReport and Occurrence "
        "where those rows are missing (e.g. after legacy data migration)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Count missing relations without modifying the database.",
        )

    def _fix_relations(self, parent_model, dry_run):
        """
        For each relation in _OCR_OCC_CHILD_RELATIONS, count (and optionally create)
        missing child rows for the given parent model.

        In dry-run mode only counts; otherwise delegates to fix_missing_occurrence_relations.
        Returns the total number of rows created (or that would be created).
        """
        parent_name = parent_model.__name__
        total_parents = parent_model.objects.count()
        self.stdout.write(f"\n{parent_name} ({total_parents} records):")

        created_count = 0

        for rel_name in _OCR_OCC_CHILD_RELATIONS:
            child_rel = parent_model._meta.get_field(rel_name)
            child_model = child_rel.related_model

            # One LEFT JOIN query to count parent records missing this child row.
            missing_qs = parent_model.objects.filter(**{f"{rel_name}__isnull": True})
            exclusion = _RELATION_GROUP_EXCLUSIONS.get(rel_name)
            if exclusion:
                missing_qs = missing_qs.exclude(**exclusion)
            count = missing_qs.count()
            created_count += count

            label = f"  {child_model.__name__}: {count} missing"
            if count == 0:
                self.stdout.write(label)
            else:
                self.stdout.write(self.style.WARNING(label))

        return created_count

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        if dry_run:
            self.stdout.write(self.style.WARNING("!!! DRY RUN MODE ACTIVE !!!"))

        with transaction.atomic():
            ocr_count = self._fix_relations(OccurrenceReport, dry_run)
            occ_count = self._fix_relations(Occurrence, dry_run)

            if dry_run:
                transaction.set_rollback(True)
            else:
                counts = fix_missing_occurrence_relations()
                ocr_count = counts["ocr"]
                occ_count = counts["occ"]

        total = ocr_count + occ_count
        status_word = "Found" if dry_run else "Successfully created"
        self.stdout.write(
            self.style.SUCCESS(
                f"\n{status_word} {total} missing relations ({ocr_count} OccurrenceReport, {occ_count} Occurrence)."
            )
        )
