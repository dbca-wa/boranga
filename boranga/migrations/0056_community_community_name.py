# Generated by Django 3.2.12 on 2022-08-11 07:51

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0055_remove_community_community_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='community',
            name='community_name',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='boranga.communityname'),
        ),
    ]
