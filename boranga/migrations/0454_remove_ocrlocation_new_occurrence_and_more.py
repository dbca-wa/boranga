# Generated by Django 5.0.9 on 2024-09-06 01:54

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0453_remove_ocrlocation_boundary_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="ocrlocation",
            name="new_occurrence",
        ),
    ]
