# Generated by Django 3.2.16 on 2023-06-16 05:21

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0132_agendaitem'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='agendaitem',
            unique_together={('meeting', 'conservation_status')},
        ),
    ]
