# Generated by Django 3.2.20 on 2023-09-19 03:55

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0174_auto_20230919_1027'),
    ]

    operations = [
        migrations.CreateModel(
            name='IdentificationCertainty',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250, unique=True)),
            ],
            options={
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='SampleDestination',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250)),
            ],
            options={
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='SampleType',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250)),
                ('group_type', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.grouptype')),
            ],
            options={
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='PermitType',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=250)),
                ('group_type', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.grouptype')),
            ],
            options={
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='Identification',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('id_confirmed_by', models.CharField(blank=True, max_length=1000, null=True)),
                ('permit_id', models.CharField(blank=True, max_length=500, null=True)),
                ('collector_number', models.CharField(blank=True, max_length=500, null=True)),
                ('barcode_number', models.CharField(blank=True, max_length=500, null=True)),
                ('identification_comment', models.TextField(blank=True, null=True)),
                ('identification_certainty', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.identificationcertainty')),
                ('occurrence_report', models.OneToOneField(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='identification', to='boranga.occurrencereport')),
                ('permit_type', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.permittype')),
                ('sample_destination', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.sampledestination')),
                ('sample_type', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.sampletype')),
            ],
        ),
    ]
