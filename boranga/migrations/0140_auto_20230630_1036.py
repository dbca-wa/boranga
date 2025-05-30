# Generated by Django 3.2.16 on 2023-06-30 02:36

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0139_remove_agendaitem_unique agenda order per meeting"),
    ]

    operations = [
        migrations.CreateModel(
            name="PorposalAmendmentReason",
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
                ("reason", models.CharField(max_length=125, verbose_name="Reason")),
            ],
            options={
                "verbose_name": "Application Amendment Reason",
                "verbose_name_plural": "Application Amendment Reasons",
            },
        ),
        migrations.AlterField(
            model_name="conservationstatus",
            name="customer_status",
            field=models.CharField(
                choices=[
                    ("draft", "Draft"),
                    ("with_assessor", "Under Review"),
                    ("ready_for_agenda", "In Meeting"),
                    ("amendment_required", "Amendment Required"),
                    ("approved", "Approved"),
                    ("declined", "Declined"),
                    ("discarded", "Discarded"),
                    ("closed", "DeListed"),
                    ("partially_approved", "Partially Approved"),
                    ("partially_declined", "Partially Declined"),
                ],
                default="draft",
                max_length=40,
                verbose_name="Customer Status",
            ),
        ),
        # Changed from alter field to add field as the migration that originally created this field
        # has been commented out as it was referring to a model that no longer exists.
        migrations.AddField(
            model_name="conservationstatusamendmentrequest",
            name="reason",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                to="boranga.porposalamendmentreason",
            ),
        ),
    ]
