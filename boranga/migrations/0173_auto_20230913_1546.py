# Generated by Django 3.2.20 on 2023-09-13 07:46

from django.db import migrations, models
import django.db.models.deletion
import multiselectfield.db.fields


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0172_auto_20230913_1519'),
    ]

    operations = [
        migrations.CreateModel(
            name='AnimalHealth',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250, unique=True)),
            ],
            options={
                'verbose_name': 'Animal Health',
                'verbose_name_plural': 'Animal Health',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='DeathReason',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250, unique=True)),
            ],
            options={
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='PrimaryDetectionMethod',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250, unique=True)),
            ],
            options={
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='ReproductiveMaturity',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250, unique=True)),
            ],
            options={
                'verbose_name': 'Reproductive Maturity',
                'verbose_name_plural': 'Reproductive Maturities',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='SecondarySign',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250, unique=True)),
            ],
            options={
                'ordering': ['name'],
            },
        ),
        migrations.AlterField(
            model_name='observationdetail',
            name='occurrence_report',
            field=models.OneToOneField(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='observation_detail', to='boranga.occurrencereport'),
        ),
        migrations.CreateModel(
            name='AnimalObservation',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('primary_detection_method', multiselectfield.db.fields.MultiSelectField(blank=True, choices=[], max_length=250, null=True)),
                ('reproductive_maturity', multiselectfield.db.fields.MultiSelectField(blank=True, choices=[], max_length=250, null=True)),
                ('secondary_sign', multiselectfield.db.fields.MultiSelectField(blank=True, choices=[], max_length=250, null=True)),
                ('total_count', models.IntegerField(blank=True, default=0, null=True)),
                ('distinctive_feature', models.CharField(blank=True, max_length=1000, null=True)),
                ('action_taken', models.CharField(blank=True, max_length=1000, null=True)),
                ('action_required', models.CharField(blank=True, max_length=1000, null=True)),
                ('observation_detail_comment', models.CharField(blank=True, max_length=1000, null=True)),
                ('animal_health', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.animalhealth')),
                ('death_reason', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.deathreason')),
                ('occurrence_report', models.OneToOneField(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='animal_observation', to='boranga.occurrencereport')),
            ],
        ),
    ]
