import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        (
            "boranga",
            "0667_jobqueue_retry_count_alter_communitydocument__file_and_more",
        ),
    ]

    operations = [
        migrations.AddField(
            model_name="occurrencereportbulkimporttask",
            name="history_seeded",
            field=models.BooleanField(default=False),
        ),
    ]
