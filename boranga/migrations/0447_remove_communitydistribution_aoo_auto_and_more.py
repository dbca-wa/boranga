# Generated by Django 5.0.8 on 2024-09-05 04:05

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0446_remove_communitydistribution_department_file_numbers_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='communitydistribution',
            name='aoo_auto',
        ),
        migrations.RemoveField(
            model_name='speciesdistribution',
            name='aoo_auto',
        ),
    ]
