# Generated by Django 3.2.25 on 2024-07-22 02:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0364_merge_0362_auto_20240719_1506_0363_auto_20240719_1410'),
    ]

    operations = [
        migrations.AddField(
            model_name='occurrencetenurepurpose',
            name='code',
            field=models.CharField(blank=True, max_length=20, null=True),
        ),
        migrations.AddField(
            model_name='occurrencetenurevesting',
            name='code',
            field=models.CharField(blank=True, max_length=20, null=True),
        ),
    ]
