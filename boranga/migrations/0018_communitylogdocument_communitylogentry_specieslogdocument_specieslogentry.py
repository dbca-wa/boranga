# Generated by Django 3.2.12 on 2022-06-13 08:51

import boranga.components.species_and_communities.models
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0017_auto_20220610_1700'),
    ]

    operations = [
        migrations.CreateModel(
            name='SpeciesLogEntry',
            fields=[
                ('communicationslogentry_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='boranga.communicationslogentry')),
                ('species', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='comms_logs', to='boranga.species')),
            ],
            bases=('boranga.communicationslogentry',),
        ),
        migrations.CreateModel(
            name='SpeciesLogDocument',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255, verbose_name='name')),
                ('description', models.TextField(blank=True, verbose_name='description')),
                ('uploaded_date', models.DateTimeField(auto_now_add=True)),
                ('_file', models.FileField(max_length=512, upload_to=boranga.components.species_and_communities.models.update_species_comms_log_filename)),
                ('log_entry', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='documents', to='boranga.specieslogentry')),
            ],
        ),
        migrations.CreateModel(
            name='CommunityLogEntry',
            fields=[
                ('communicationslogentry_ptr', models.OneToOneField(auto_created=True, on_delete=django.db.models.deletion.CASCADE, parent_link=True, primary_key=True, serialize=False, to='boranga.communicationslogentry')),
                ('community', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='comms_logs', to='boranga.community')),
            ],
            bases=('boranga.communicationslogentry',),
        ),
        migrations.CreateModel(
            name='CommunityLogDocument',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255, verbose_name='name')),
                ('description', models.TextField(blank=True, verbose_name='description')),
                ('uploaded_date', models.DateTimeField(auto_now_add=True)),
                ('_file', models.FileField(max_length=512, upload_to=boranga.components.species_and_communities.models.update_community_comms_log_filename)),
                ('log_entry', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='documents', to='boranga.communitylogentry')),
            ],
        ),
    ]
