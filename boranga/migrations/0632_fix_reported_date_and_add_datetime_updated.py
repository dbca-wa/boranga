"""
Migration 0632 — two related changes to OccurrenceReport:

1. reported_date: change from auto_now_add=True to default=timezone.now so that
   legacy data migration adapters can supply historical creation dates.

2. datetime_updated: new field with default=timezone.now, auto-set in save().
   Backfill: set to the most recent OccurrenceReportUserAction.when per OCR,
   falling back to reported_date for records with no action log entry.
"""

from django.db import migrations, models
import django.utils.timezone


def backfill_datetime_updated(apps, schema_editor):
    OccurrenceReport = apps.get_model("boranga", "OccurrenceReport")
    OccurrenceReportUserAction = apps.get_model("boranga", "OccurrenceReportUserAction")

    # Build a map of occurrence_report_id → most recent action when
    most_recent = {}
    for action in OccurrenceReportUserAction.objects.order_by("occurrence_report_id", "-when").values(
        "occurrence_report_id", "when"
    ):
        ocr_id = action["occurrence_report_id"]
        if ocr_id not in most_recent:
            most_recent[ocr_id] = action["when"]

    batch = []
    for ocr in OccurrenceReport.objects.only("id", "reported_date"):
        ocr.datetime_updated = most_recent.get(ocr.pk) or ocr.reported_date or django.utils.timezone.now()
        batch.append(ocr)
        if len(batch) >= 500:
            OccurrenceReport.objects.bulk_update(batch, ["datetime_updated"])
            batch.clear()

    if batch:
        OccurrenceReport.objects.bulk_update(batch, ["datetime_updated"])


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0631_add_datetime_approved_to_occurrence_report"),
    ]

    operations = [
        migrations.AlterField(
            model_name="occurrencereport",
            name="reported_date",
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
        migrations.AddField(
            model_name="occurrencereport",
            name="datetime_updated",
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
        migrations.RunPython(backfill_datetime_updated, noop),
    ]
