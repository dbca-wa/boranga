# Generated by Django 3.2.25 on 2024-08-07 06:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0395_merge_20240806_1637'),
    ]

    operations = [
        migrations.AddField(
            model_name='tilelayer',
            name='matrix_set',
            field=models.CharField(blank=True, max_length=255),
        ),
        migrations.AddField(
            model_name='tilelayer',
            name='service',
            field=models.CharField(choices=[('wms', 'WMS'), ('wmts', 'WMTS')], default='wms', max_length=10),
        ),
        migrations.AddField(
            model_name='tilelayer',
            name='tile_pixel_size',
            field=models.PositiveIntegerField(default=256),
        ),
    ]
