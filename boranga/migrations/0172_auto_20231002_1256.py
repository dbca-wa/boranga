# Generated by Django 3.2.20 on 2023-10-02 04:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0171_observationdetails_observationmethod'),
    ]

    operations = [
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='average_lifespan_choice',
            field=models.CharField(blank=True, choices=[(1, 'year/s'), (2, 'month/s')], max_length=10, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='average_lifespan_from',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='average_lifespan_to',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='generation_length_choice',
            field=models.CharField(blank=True, choices=[(1, 'year/s'), (2, 'month/s')], max_length=10, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='generation_length_from',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='generation_length_to',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='time_to_maturity_choice',
            field=models.CharField(blank=True, choices=[(1, 'year/s'), (2, 'month/s')], max_length=10, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='time_to_maturity_from',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationattributes',
            name='time_to_maturity_to',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
