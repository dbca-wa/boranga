from django.db import migrations


def set_approved_ocrs_locked(apps, schema_editor):
    OccurrenceReport = apps.get_model("boranga", "OccurrenceReport")
    updated = OccurrenceReport.objects.filter(
        processing_status="approved",
        locked=False,
    ).update(locked=True)
    if updated:
        print(f"\n    Set locked=True on {updated} approved OccurrenceReport(s)")


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0645_occurrencereport_locked_and_more"),
    ]

    operations = [
        migrations.RunPython(
            set_approved_ocrs_locked,
            migrations.RunPython.noop,
        ),
    ]
