# Generated by Django 3.2.12 on 2022-06-24 05:49

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0026_alter_taxonomy_species'),
    ]

    operations = [
        migrations.AlterField(
            model_name='taxonomy',
            name='species',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='species_taxonomy', to='boranga.species', unique=True),
        ),
    ]
