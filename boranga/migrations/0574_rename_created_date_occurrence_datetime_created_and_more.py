# Generated by Django 5.2.4 on 2025-07-11 00:53

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0573_conservationstatus_datetime_created_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="occurrence",
            old_name="created_date",
            new_name="datetime_created",
        ),
        migrations.RenameField(
            model_name="occurrence",
            old_name="updated_date",
            new_name="datetime_updated",
        ),
    ]
