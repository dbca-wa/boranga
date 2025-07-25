# Generated by Django 5.2.4 on 2025-07-22 09:31

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0580_rename_floragroup_faunagroup_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="species",
            name="fauna_group",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="species",
                to="boranga.faunagroup",
            ),
        ),
        migrations.AddField(
            model_name="species",
            name="fauna_sub_group",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="species",
                to="boranga.faunasubgroup",
            ),
        ),
    ]
