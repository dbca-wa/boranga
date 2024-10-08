# Generated by Django 5.0.8 on 2024-08-29 23:59

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0440_occurrencereportbulkimportschema_name_and_more"),
        ("contenttypes", "0002_remove_content_type_name"),
    ]

    operations = [
        migrations.AddConstraint(
            model_name="occurrencereportbulkimportschemacolumn",
            constraint=models.UniqueConstraint(
                fields=(
                    "schema",
                    "django_import_content_type",
                    "django_import_field_name",
                ),
                name="unique_schema_column_import",
                violation_error_message="This field already exists in the schema",
            ),
        ),
        migrations.AddConstraint(
            model_name="occurrencereportbulkimportschemacolumn",
            constraint=models.UniqueConstraint(
                fields=("schema", "xlsx_column_header_name"),
                name="unique_schema_column_header",
                violation_error_message="This column name already exists in the schema",
            ),
        ),
    ]
