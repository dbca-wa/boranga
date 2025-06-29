# Generated by Django 5.2.3 on 2025-06-26 06:49

import django.core.validators
from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0556_alter_communitydocument__file_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="occplantcount",
            name="individual_quadrat_area",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                max_digits=12,
                null=True,
                validators=[django.core.validators.MinValueValidator(Decimal("0.00"))],
            ),
        ),
        migrations.AlterField(
            model_name="ocrplantcount",
            name="individual_quadrat_area",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                max_digits=12,
                null=True,
                validators=[django.core.validators.MinValueValidator(Decimal("0.00"))],
            ),
        ),
    ]
