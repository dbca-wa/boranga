# Generated by Django 3.2.12 on 2022-06-24 05:30

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0025_auto_20220624_1106'),
    ]

    operations = [
        migrations.AlterField(
            model_name='taxonomy',
            name='species',
            field=models.OneToOneField(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='species_taxonomy', to='boranga.species'),
        ),
    ]
