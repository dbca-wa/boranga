from django.db import migrations


def fix_stale_allow_blank(apps, schema_editor):
    """
    When a Django model field changes from null=True to null=False (or gains a
    default), existing OccurrenceReportBulkImportSchemaColumn rows may still
    have xlsx_data_validation_allow_blank=True — a stale value from before the
    field change.  This migration corrects those rows by setting allow_blank to
    False for every column whose target field is now mandatory (not null, not
    blank, and no default).
    """
    OccurrenceReportBulkImportSchemaColumn = apps.get_model(
        "boranga", "OccurrenceReportBulkImportSchemaColumn"
    )

    stale_columns = OccurrenceReportBulkImportSchemaColumn.objects.filter(
        xlsx_data_validation_allow_blank=True,
        django_import_content_type__isnull=False,
    ).select_related("django_import_content_type")

    for column in stale_columns:
        try:
            model_class = column.django_import_content_type.model_class()
            if model_class is None:
                continue
            field = model_class._meta.get_field(column.django_import_field_name)
        except Exception:
            # Field no longer exists or content type is stale — skip.
            continue

        null = getattr(field, "null", False)
        blank = getattr(field, "blank", False)
        has_default = field.has_default() if hasattr(field, "has_default") else False

        if not null and not blank and not has_default:
            # Use .filter().update() rather than bulk_update: historical model
            # instances from apps.get_model() don't reliably flush via
            # bulk_update, but a queryset .update() always issues the SQL.
            OccurrenceReportBulkImportSchemaColumn.objects.filter(
                pk=column.pk
            ).update(xlsx_data_validation_allow_blank=False)


class Migration(migrations.Migration):

    dependencies = [
        (
            "boranga",
            "0660_alter_communitydocument__file_alter_minutes__file_and_more",
        ),
    ]

    operations = [
        migrations.RunPython(
            fix_stale_allow_blank,
            migrations.RunPython.noop,
        ),
    ]
