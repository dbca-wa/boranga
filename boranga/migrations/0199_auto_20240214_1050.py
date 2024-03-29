# Generated by Django 3.2.23 on 2024-02-14 02:50

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0198_auto_20240213_1641'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='conservationplancommunity',
            name='community',
        ),
        migrations.RemoveField(
            model_name='conservationplancommunity',
            name='conservation_plan',
        ),
        migrations.RemoveField(
            model_name='conservationplanspecies',
            name='conservation_plan',
        ),
        migrations.RemoveField(
            model_name='conservationplanspecies',
            name='species',
        ),
        migrations.DeleteModel(
            name='ConservationPlan',
        ),
        migrations.DeleteModel(
            name='ConservationPlanCommunity',
        ),
        migrations.DeleteModel(
            name='ConservationPlanSpecies',
        ),
        migrations.DeleteModel(
            name='PlanType',
        ),
    ]
