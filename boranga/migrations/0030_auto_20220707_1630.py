# Generated by Django 3.2.12 on 2022-07-07 08:30

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0029_bconservationcategory_bconservationchangecode_bconservationcriteria_bconservationlist_bconservations'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='community',
            name='conservation_status',
        ),
        migrations.RemoveField(
            model_name='species',
            name='conservation_status',
        ),
    ]
