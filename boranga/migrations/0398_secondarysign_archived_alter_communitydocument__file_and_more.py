# Generated by Django 5.0.8 on 2024-08-07 05:52

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0397_sitetype_archived_alter_communitydocument__file_and_more"),
        ("contenttypes", "0002_remove_content_type_name"),
    ]

    operations = [
        migrations.AddField(
            model_name="secondarysign",
            name="archived",
            field=models.BooleanField(default=False),
        ),
    ]