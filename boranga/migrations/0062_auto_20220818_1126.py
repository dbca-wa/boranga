# Generated by Django 3.2.12 on 2022-08-18 03:26

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0061_auto_20220818_1110'),
    ]

    operations = [
        migrations.AddField(
            model_name='communityconservationstatus',
            name='proposed_conservation_category',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='communityconservationstatus_prop_conservation_category', to='boranga.conservationcategory'),
        ),
        migrations.AddField(
            model_name='communityconservationstatus',
            name='proposed_conservation_criteria',
            field=models.ManyToManyField(blank=True, null=True, related_name='communityconservationstatus_prop_conservation_criteria', to='boranga.ConservationCriteria'),
        ),
        migrations.AddField(
            model_name='communityconservationstatus',
            name='proposed_conservation_list',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='communityconservationstatus_prop_conservation_list', to='boranga.conservationlist'),
        ),
        migrations.AddField(
            model_name='speciesconservationstatus',
            name='proposed_conservation_category',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='speciesconservationstatus_prop_conservation_category', to='boranga.conservationcategory'),
        ),
        migrations.AddField(
            model_name='speciesconservationstatus',
            name='proposed_conservation_criteria',
            field=models.ManyToManyField(blank=True, null=True, related_name='speciesconservationstatus_prop_conservation_criteria', to='boranga.ConservationCriteria'),
        ),
        migrations.AddField(
            model_name='speciesconservationstatus',
            name='proposed_conservation_list',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='speciesconservationstatus_prop_conservation_list', to='boranga.conservationlist'),
        ),
        migrations.AlterField(
            model_name='communityconservationstatus',
            name='current_conservation_category',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='communityconservationstatus_curr_conservation_category', to='boranga.conservationcategory'),
        ),
        migrations.AlterField(
            model_name='communityconservationstatus',
            name='current_conservation_criteria',
            field=models.ManyToManyField(blank=True, null=True, related_name='communityconservationstatus_curr_conservation_criteria', to='boranga.ConservationCriteria'),
        ),
        migrations.AlterField(
            model_name='communityconservationstatus',
            name='current_conservation_list',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='communityconservationstatus_curr_conservation_list', to='boranga.conservationlist'),
        ),
        migrations.AlterField(
            model_name='speciesconservationstatus',
            name='current_conservation_category',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='speciesconservationstatus_curr_conservation_category', to='boranga.conservationcategory'),
        ),
        migrations.AlterField(
            model_name='speciesconservationstatus',
            name='current_conservation_criteria',
            field=models.ManyToManyField(blank=True, null=True, related_name='speciesconservationstatus_curr_conservation_criteria', to='boranga.ConservationCriteria'),
        ),
        migrations.AlterField(
            model_name='speciesconservationstatus',
            name='current_conservation_list',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='speciesconservationstatus_curr_conservation_list', to='boranga.conservationlist'),
        ),
    ]
