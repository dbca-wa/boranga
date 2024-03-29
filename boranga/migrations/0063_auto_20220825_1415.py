# Generated by Django 3.2.12 on 2022-08-25 06:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0062_auto_20220818_1126'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='family',
            options={'verbose_name': 'Family', 'verbose_name_plural': 'Families'},
        ),
        migrations.AlterModelOptions(
            name='genus',
            options={'verbose_name': 'Genus', 'verbose_name_plural': 'Genera'},
        ),
        migrations.AddField(
            model_name='speciesconservationstatus',
            name='curr_conservation_criteria',
            field=models.ManyToManyField(blank=True, null=True, related_name='species_curr_conservation_criteria', to='boranga.ConservationCriteria'),
        ),
    ]
