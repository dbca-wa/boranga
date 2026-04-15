from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0647_fix_community_lookup_field_name"),
    ]

    operations = [
        migrations.RenameField(
            model_name="occurrencereport",
            old_name="reported_date",
            new_name="datetime_created",
        ),
    ]
