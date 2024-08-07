# Generated by Django 5.0.8 on 2024-08-07 04:22

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0395_merge_20240806_1637'),
        ('contenttypes', '0002_remove_content_type_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='soilcolour',
            name='archived',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='buffergeometry',
            name='content_type',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='content_type_%(class)s', to='contenttypes.contenttype'),
        ),
        migrations.AlterField(
            model_name='community',
            name='processing_status',
            field=models.CharField(choices=[('draft', 'Draft'), ('discarded', 'Discarded'), ('active', 'Active'), ('historical', 'Historical')], default='draft', max_length=30, verbose_name='Processing Status'),
        ),
        migrations.AlterField(
            model_name='occurrencesite',
            name='content_type',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='content_type_%(class)s', to='contenttypes.contenttype'),
        ),
        migrations.AlterField(
            model_name='species',
            name='parent_species',
            field=models.ManyToManyField(blank=True, related_name='parent', to='boranga.species'),
        ),
        migrations.AlterField(
            model_name='species',
            name='processing_status',
            field=models.CharField(blank=True, choices=[('draft', 'Draft'), ('discarded', 'Discarded'), ('active', 'Active'), ('historical', 'Historical')], default='draft', max_length=30, null=True, verbose_name='Processing Status'),
        ),
    ]
