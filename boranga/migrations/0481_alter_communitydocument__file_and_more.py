# Generated by Django 5.0.9 on 2024-10-03 06:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0480_alter_communitydocument__file_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="occurrencereportbulkimportschemacolumn",
            name="default_value",
            field=models.CharField(
                blank=True,
                choices=[("bulk_import_submitter", "Bulk Import Submitter")],
                max_length=255,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="schemacolumnlookupfilter",
            name="filter_type",
            field=models.CharField(
                choices=[
                    ("exact", "Exact"),
                    ("iexact", "Case-insensitive Exact"),
                    ("contains", "Contains"),
                    ("icontains", "Case-insensitive Contains"),
                    ("startswith", "Starts with"),
                    ("istartswith", "Case-insensitive Starts with"),
                    ("endswith", "Ends with"),
                    ("iendswith", "Case-insensitive Ends with"),
                    ("gt", "Greater than"),
                    ("gte", "Greater than or equal to"),
                    ("lt", "Less than"),
                    ("lte", "Less than or equal to"),
                ],
                default="exact",
                max_length=50,
            ),
        ),
    ]
