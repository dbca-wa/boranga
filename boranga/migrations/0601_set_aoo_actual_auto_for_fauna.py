from django.db import migrations


def set_aoo_actual_auto_for_fauna(apps, schema_editor):
    """Set aoo_actual_auto=False for SpeciesDistribution rows whose Species is fauna.

    This is a safe, idempotent forward migration. The reverse is a noop because
    we cannot reliably restore previous values.
    """
    SpeciesDistribution = apps.get_model("boranga", "SpeciesDistribution")

    # Filter distributions where the related species has group_type name 'fauna'
    qs = SpeciesDistribution.objects.filter(species__group_type__name="fauna")

    # Bulk update them to False. This will execute a single UPDATE query.
    qs.update(aoo_actual_auto=False)


def noop_reverse(apps, schema_editor):
    # Intentionally do nothing on reverse; data cannot be reliably restored.
    return


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0600_merge_20251016_1659"),
    ]

    operations = [
        migrations.RunPython(set_aoo_actual_auto_for_fauna, reverse_code=noop_reverse),
    ]
