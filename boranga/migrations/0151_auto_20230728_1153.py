# Generated by Django 3.2.20 on 2023-07-28 03:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0150_auto_20230726_1617'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='communityconservationattributes',
            name='habitat_growth_form',
        ),
        migrations.AlterField(
            model_name='communityconservationattributes',
            name='pollinator_information',
            field=models.CharField(blank=True, max_length=1000, null=True),
        ),
    ]
