# Generated by Django 3.2.16 on 2022-11-18 02:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0082_conservationstatusdeclineddetails'),
    ]

    operations = [
        migrations.AddField(
            model_name='conservationstatus',
            name='proposed_decline_status',
            field=models.BooleanField(default=False),
        ),
    ]
