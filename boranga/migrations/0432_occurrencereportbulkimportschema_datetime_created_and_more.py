# Generated by Django 5.0.8 on 2024-08-19 03:25

import datetime
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0431_occurrencereportbulkimporttask_archived_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="occurrencereportbulkimportschema",
            name="datetime_created",
            field=models.DateTimeField(
                auto_now_add=True, default=django.utils.timezone.now
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="occurrencereportbulkimportschema",
            name="datetime_updated",
            field=models.DateTimeField(default=datetime.datetime.now),
        ),
        migrations.AlterField(
            model_name="occurrencereportbulkimportschemacolumn",
            name="data_validation_type",
            field=models.CharField(
                choices=[
                    ("whole", "whole"),
                    ("time", "time"),
                    ("decimal", "decimal"),
                    ("custom", "custom"),
                    ("list", "list"),
                    ("date", "date"),
                    ("textLength", "textLength"),
                    (None, None),
                ],
                default="string",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="occurrencereportbulkimporttask",
            name="processing_status",
            field=models.CharField(
                choices=[
                    ("queued", "Queued"),
                    ("started", "Started"),
                    ("failed", "Failed"),
                    ("completed", "Completed"),
                    ("archived", "Archived"),
                ],
                default="queued",
                max_length=20,
            ),
        ),
    ]