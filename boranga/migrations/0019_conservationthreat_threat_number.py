# Generated by Django 3.2.12 on 2022-06-15 06:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0018_auto_20220615_1208'),
    ]

    operations = [
        migrations.AddField(
            model_name='conservationthreat',
            name='threat_number',
            field=models.CharField(blank=True, default='', max_length=9),
        ),
    ]
