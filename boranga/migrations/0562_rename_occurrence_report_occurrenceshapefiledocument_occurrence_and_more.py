# Generated by Django 5.2.3 on 2025-07-02 01:45

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0561_alter_communitydocument__file_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="occurrenceshapefiledocument",
            old_name="occurrence_report",
            new_name="occurrence",
        ),
    ]
