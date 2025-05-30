# Generated by Django 5.0.9 on 2024-11-12 05:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        (
            "boranga",
            "0492_rename_international_conservation_conservationstatus_other_conservation_assessment_and_more",
        ),
    ]

    operations = [
        migrations.CreateModel(
            name="IUCNVersion",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("archived", models.BooleanField(default=False)),
                ("code", models.CharField(max_length=64)),
                ("label", models.CharField(max_length=512)),
                ("applies_to_flora", models.BooleanField(default=False)),
                ("applies_to_fauna", models.BooleanField(default=False)),
                ("applies_to_communities", models.BooleanField(default=False)),
            ],
            options={
                "verbose_name": "IU",
                "ordering": ["code"],
            },
        ),
    ]
