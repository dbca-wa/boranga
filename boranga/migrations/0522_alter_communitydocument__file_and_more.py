# Generated by Django 5.2 on 2025-05-05 07:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0521_alter_communitydocument__file_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_alive_juvenile',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_alive_mature',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_alive_seedling',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_alive_unknown',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_dead_juvenile',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_dead_mature',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_dead_seedling',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='detailed_dead_unknown',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='simple_alive',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='occplantcount',
            name='simple_dead',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_alive_juvenile',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_alive_mature',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_alive_seedling',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_alive_unknown',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_dead_juvenile',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_dead_mature',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_dead_seedling',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='detailed_dead_unknown',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='simple_alive',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='ocrplantcount',
            name='simple_dead',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
