# Generated by Django 3.2.12 on 2022-08-26 06:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0065_speciesconservationstatuslogdocument_speciesconservationstatuslogentry'),
    ]

    operations = [
        migrations.AddField(
            model_name='communityconservationstatus',
            name='submitter',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='speciesconservationstatus',
            name='submitter',
            field=models.IntegerField(null=True),
        ),
    ]
