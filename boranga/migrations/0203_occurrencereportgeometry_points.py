# Generated by Django 3.2.23 on 2024-03-20 06:51

import django.contrib.gis.db.models.fields
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0202_auto_20240216_1541'),
    ]

    operations = [
        migrations.AddField(
            model_name='occurrencereportgeometry',
            name='points',
            field=django.contrib.gis.db.models.fields.PointField(blank=True, null=True, srid=4326),
        ),
    ]
