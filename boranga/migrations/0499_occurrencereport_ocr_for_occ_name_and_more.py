# Generated by Django 5.0.9 on 2024-11-18 03:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0498_occurrencereportapprovaldetails_cc_email_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="occurrencereport",
            name="ocr_for_occ_name",
            field=models.CharField(blank=True, default="", max_length=100),
        ),
        migrations.AddField(
            model_name="occurrencereport",
            name="ocr_for_occ_number",
            field=models.CharField(blank=True, default="", max_length=9),
        ),
    ]
