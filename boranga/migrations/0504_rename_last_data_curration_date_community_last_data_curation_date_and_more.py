# Generated by Django 5.0.10 on 2025-01-03 07:27

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0503_alter_communitydocument__file_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="community",
            old_name="last_data_curration_date",
            new_name="last_data_curation_date",
        ),
        migrations.RenameField(
            model_name="species",
            old_name="last_data_curration_date",
            new_name="last_data_curation_date",
        ),
    ]
