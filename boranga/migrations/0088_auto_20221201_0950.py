# Generated by Django 3.2.16 on 2022-12-01 01:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0087_conservationstatusissuanceapprovaldetails_cc_email'),
    ]

    operations = [
        migrations.AlterField(
            model_name='conservationstatusissuanceapprovaldetails',
            name='effective_from_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='conservationstatusissuanceapprovaldetails',
            name='effective_to_date',
            field=models.DateField(blank=True, null=True),
        ),
    ]
