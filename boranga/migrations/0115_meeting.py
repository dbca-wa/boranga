# Generated by Django 3.2.16 on 2023-03-09 04:00

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0114_crossreference'),
    ]

    operations = [
        migrations.CreateModel(
            name='Meeting',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('start_date', models.DateTimeField(blank=True, null=True)),
                ('end_date', models.DateTimeField(blank=True, null=True)),
                ('location', models.CharField(blank=True, max_length=128, null=True)),
                ('title', models.CharField(blank=True, max_length=128, null=True)),
            ],
        ),
    ]
