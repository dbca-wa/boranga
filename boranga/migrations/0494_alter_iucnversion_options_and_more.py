# Generated by Django 5.0.9 on 2024-11-12 05:58

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0493_iucnversion_alter_communitydocument__file_and_more"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="iucnversion",
            options={"ordering": ["code"], "verbose_name": "IUCN Version"},
        ),
    ]