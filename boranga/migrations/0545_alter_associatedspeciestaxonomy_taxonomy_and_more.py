# Generated by Django 5.2.3 on 2025-06-12 02:20

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0544_ocrassociatedspecies_related_species_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="associatedspeciestaxonomy",
            name="taxonomy",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="associated_species_taxonomy",
                to="boranga.taxonomy",
            ),
        ),
    ]
