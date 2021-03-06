# Generated by Django 3.2.12 on 2022-06-22 05:41

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0021_delete_conservationattributes'),
    ]

    operations = [
        migrations.AlterField(
            model_name='conservationattributestemp',
            name='species',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, primary_key=True, serialize=False, to='boranga.species'),
        ),
        migrations.CreateModel(
            name='ConservationAttributes',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('general_management_advice', models.CharField(blank=True, default='None', max_length=512, null=True)),
                ('ecological_attributes', models.CharField(blank=True, default='None', max_length=512, null=True)),
                ('biological_attributes', models.CharField(blank=True, default='None', max_length=512, null=True)),
                ('specific_survey_advice', models.CharField(blank=True, default='None', max_length=512, null=True)),
                ('comments', models.CharField(blank=True, default='None', max_length=2048, null=True)),
                ('species', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='species_conservation_attributes', to='boranga.species', unique=True)),
            ],
        ),
    ]
