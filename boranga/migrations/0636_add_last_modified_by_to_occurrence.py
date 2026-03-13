"""
Migration 0636 — add last_modified_by to Occurrence.

Backfill: populate from the most recent OccurrenceUserAction.who
per Occurrence.
"""

from django.db import migrations, models


def backfill_last_modified_by(apps, schema_editor):
    Occurrence = apps.get_model("boranga", "Occurrence")
    OccurrenceUserAction = apps.get_model("boranga", "OccurrenceUserAction")

    most_recent_who: dict[int, int] = {}
    for action in OccurrenceUserAction.objects.order_by(
        "occurrence_id", "-when"
    ).values("occurrence_id", "who"):
        occ_id = action["occurrence_id"]
        if occ_id not in most_recent_who and action["who"] is not None:
            most_recent_who[occ_id] = action["who"]

    batch = []
    for occ in Occurrence.objects.only("id", "last_modified_by"):
        who = most_recent_who.get(occ.pk)
        if who is not None:
            occ.last_modified_by = who
            batch.append(occ)
        if len(batch) >= 500:
            Occurrence.objects.bulk_update(batch, ["last_modified_by"])
            batch.clear()

    if batch:
        Occurrence.objects.bulk_update(batch, ["last_modified_by"])


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0635_remove_geoserverurl_wms_version"),
    ]

    operations = [
        migrations.AddField(
            model_name="occurrence",
            name="last_modified_by",
            field=models.IntegerField(null=True),
        ),
        migrations.RunPython(backfill_last_modified_by, noop),
    ]
