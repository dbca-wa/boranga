"""
Migration 0633 — add last_modified_by to OccurrenceReport.

Replaces the action-log Subquery hack in the filter backend for
filter_last_modified_by with a direct IntegerField on the model.

Backfill: populate from the most recent OccurrenceReportUserAction.who
per OCR (same source as the old Subquery).
"""

from django.db import migrations, models


def backfill_last_modified_by(apps, schema_editor):
    OccurrenceReport = apps.get_model("boranga", "OccurrenceReport")
    OccurrenceReportUserAction = apps.get_model("boranga", "OccurrenceReportUserAction")

    most_recent_who: dict[int, int] = {}
    for action in OccurrenceReportUserAction.objects.order_by(
        "occurrence_report_id", "-when"
    ).values("occurrence_report_id", "who"):
        ocr_id = action["occurrence_report_id"]
        if ocr_id not in most_recent_who and action["who"] is not None:
            most_recent_who[ocr_id] = action["who"]

    batch = []
    for ocr in OccurrenceReport.objects.only("id", "last_modified_by"):
        who = most_recent_who.get(ocr.pk)
        if who is not None:
            ocr.last_modified_by = who
            batch.append(ocr)
        if len(batch) >= 500:
            OccurrenceReport.objects.bulk_update(batch, ["last_modified_by"])
            batch.clear()

    if batch:
        OccurrenceReport.objects.bulk_update(batch, ["last_modified_by"])


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0632_fix_reported_date_and_add_datetime_updated"),
    ]

    operations = [
        migrations.AddField(
            model_name="occurrencereport",
            name="last_modified_by",
            field=models.IntegerField(null=True),
        ),
        migrations.RunPython(backfill_last_modified_by, noop),
    ]
