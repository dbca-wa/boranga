# Generated by Django 3.2.16 on 2023-05-31 03:53

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0127_auto_20230531_1059'),
    ]

    operations = [
        migrations.RenameField(
            model_name='meeting',
            old_name='attendess',
            new_name='attendees',
        ),
    ]
