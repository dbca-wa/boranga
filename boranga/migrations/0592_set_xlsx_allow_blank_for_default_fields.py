"""Make schema columns allow blanks when their model field has a default value.

This data migration inspects all OccurrenceReportBulkImportSchemaColumn rows
and sets xlsx_data_validation_allow_blank=True when the underlying Django
model field for that column has a default (i.e. field.default is not
NOT_PROVIDED).

The reverse migration is a no-op to avoid destroying user configuration.
"""
from django.db import migrations


def set_allow_blank_for_default_fields(apps, schema_editor):
    ContentType = apps.get_model("contenttypes", "ContentType")
    SchemaColumn = apps.get_model(
        "boranga", "OccurrenceReportBulkImportSchemaColumn"
    )

    # Import here to use runtime constant
    try:
        from django.db.models.fields import NOT_PROVIDED
    except Exception:
        NOT_PROVIDED = object()

    for col in SchemaColumn.objects.select_related("django_import_content_type").all():
        ct = col.django_import_content_type
        if not ct:
            continue

        # Resolve the mapped model using the recorded app_label/model on the ContentType
        app_label = getattr(ct, "app_label", None)
        model_name = getattr(ct, "model", None)
        if not app_label or not model_name:
            continue

        try:
            model = apps.get_model(app_label, model_name)
        except LookupError:
            continue

        field_name = getattr(col, "django_import_field_name", None)
        if not field_name:
            continue

        try:
            field = model._meta.get_field(field_name)
        except Exception:
            continue

        default = getattr(field, "default", NOT_PROVIDED)
        if default is not NOT_PROVIDED:
            if not col.xlsx_data_validation_allow_blank:
                col.xlsx_data_validation_allow_blank = True
                col.save()


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0591_legacyusernameemailusermapping_and_more"),
    ]

    operations = [
        migrations.RunPython(
            set_allow_blank_for_default_fields,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
