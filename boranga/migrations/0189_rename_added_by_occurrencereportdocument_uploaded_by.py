# Generated by Django 3.2.23 on 2024-01-24 05:32

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0188_occurrencereportdocument_added_by'),
    ]

    operations = [
        migrations.RenameField(
            model_name='occurrencereportdocument',
            old_name='added_by',
            new_name='uploaded_by',
        ),
    ]
