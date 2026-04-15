from django.db import migrations


def fix_community_migrated_id_lookup(apps, schema_editor):
    """
    Migration 0606 renamed CommunityTaxonomy.community_migrated_id to
    community_common_id, but OccurrenceReportBulkImportSchemaColumn rows
    that stored 'taxonomy__community_migrated_id' as their
    django_lookup_field_name were not updated at the time.

    Update those rows to use the current field name.
    """
    OccurrenceReportBulkImportSchemaColumn = apps.get_model(
        "boranga", "OccurrenceReportBulkImportSchemaColumn"
    )
    OccurrenceReportBulkImportSchemaColumn.objects.filter(
        django_lookup_field_name="taxonomy__community_migrated_id"
    ).update(django_lookup_field_name="taxonomy__community_common_id")


def reverse_fix(apps, schema_editor):
    OccurrenceReportBulkImportSchemaColumn = apps.get_model(
        "boranga", "OccurrenceReportBulkImportSchemaColumn"
    )
    OccurrenceReportBulkImportSchemaColumn.objects.filter(
        django_lookup_field_name="taxonomy__community_common_id"
    ).update(django_lookup_field_name="taxonomy__community_migrated_id")


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0646_set_approved_ocrs_locked"),
    ]

    operations = [
        migrations.RunPython(fix_community_migrated_id_lookup, reverse_fix),
    ]
