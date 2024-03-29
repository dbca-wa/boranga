# Generated by Django 3.2.20 on 2023-07-11 03:50

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0142_remove_meeting_agenda'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='agendaitem',
            options={'ordering': ['meeting', 'order'], 'verbose_name': 'Meeting Agenda Item'},
        ),
        migrations.AlterModelOptions(
            name='breedingperiod',
            options={'ordering': ['period']},
        ),
        migrations.AlterModelOptions(
            name='classificationsystem',
            options={'ordering': ['class_desc']},
        ),
        migrations.AlterModelOptions(
            name='committee',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='communitytaxonomy',
            options={'ordering': ['community_name']},
        ),
        migrations.AlterModelOptions(
            name='conservationcategory',
            options={'ordering': ['code'], 'verbose_name': 'Conservation Category', 'verbose_name_plural': 'Conservation Categories'},
        ),
        migrations.AlterModelOptions(
            name='conservationcriteria',
            options={'ordering': ['code']},
        ),
        migrations.AlterModelOptions(
            name='conservationlist',
            options={'ordering': ['code']},
        ),
        migrations.AlterModelOptions(
            name='district',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='documentcategory',
            options={'ordering': ['document_category_name'], 'verbose_name': 'Document Category', 'verbose_name_plural': 'Document Categories'},
        ),
        migrations.AlterModelOptions(
            name='documentsubcategory',
            options={'ordering': ['document_sub_category_name'], 'verbose_name': 'Document Sub Category', 'verbose_name_plural': 'Document Sub Categories'},
        ),
        migrations.AlterModelOptions(
            name='faunabreeding',
            options={'ordering': ['breeding_type']},
        ),
        migrations.AlterModelOptions(
            name='florarecruitmenttype',
            options={'ordering': ['recruitment_type']},
        ),
        migrations.AlterModelOptions(
            name='floweringperiod',
            options={'ordering': ['period']},
        ),
        migrations.AlterModelOptions(
            name='fruitingperiod',
            options={'ordering': ['period']},
        ),
        migrations.AlterModelOptions(
            name='genus',
            options={'ordering': ['name'], 'verbose_name': 'Genus', 'verbose_name_plural': 'Genera'},
        ),
        migrations.AlterModelOptions(
            name='meetingroom',
            options={'ordering': ['room_name']},
        ),
        migrations.AlterModelOptions(
            name='nameauthority',
            options={'ordering': ['name'], 'verbose_name': 'Name Authority', 'verbose_name_plural': 'Name Authorities'},
        ),
        migrations.AlterModelOptions(
            name='pollinatorinformation',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='postfirehabitatinteraction',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='region',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='rootmorphology',
            options={'ordering': ['name'], 'verbose_name': 'Root Morphology', 'verbose_name_plural': 'Root Morphologies'},
        ),
        migrations.AlterModelOptions(
            name='seedviabilitygerminationinfo',
            options={'ordering': ['name']},
        ),
        migrations.AlterModelOptions(
            name='taxonomy',
            options={'ordering': ['scientific_name']},
        ),
        migrations.AlterModelOptions(
            name='taxonvernacular',
            options={'ordering': ['vernacular_name']},
        ),
    ]
