# Generated by Django 3.2.16 on 2023-01-18 02:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0094_auto_20230111_1113'),
    ]

    operations = [
        migrations.AlterField(
            model_name='taxonvernacular',
            name='taxonomy',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='vernaculars', to='boranga.taxonomy'),
        ),
    ]
