# Generated by Django 3.2.16 on 2023-01-13 08:27

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0096_auto_20230113_1407'),
    ]

    operations = [
        migrations.CreateModel(
            name='CommunityUserAction',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('who', models.IntegerField()),
                ('when', models.DateTimeField(auto_now_add=True)),
                ('what', models.TextField()),
                ('community', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='action_logs', to='boranga.community')),
            ],
            options={
                'ordering': ('-when',),
            },
        ),
    ]
