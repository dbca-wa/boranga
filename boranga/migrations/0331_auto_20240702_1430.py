# Generated by Django 3.2.25 on 2024-07-02 06:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0330_merge_20240702_1427'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='occurrencesite',
            name='point_coord1',
        ),
        migrations.RemoveField(
            model_name='occurrencesite',
            name='point_coord2',
        ),
        migrations.AddField(
            model_name='datum',
            name='srid',
            field=models.IntegerField(default=1, unique=True),
            preserve_default=False,
        ),
    ]