from django.db import migrations, models
import sys

def populate_previous_taxonomy(apps, schema_editor):
    Taxonomy = apps.get_model("boranga", "Taxonomy")
    TaxonPreviousName = apps.get_model("boranga", "TaxonPreviousName")

    # build in-memory lookup maps (one DB scan each)
    tax_by_name_id = {
        tn_id: pk
        for tn_id, pk in Taxonomy.objects.exclude(taxon_name_id__isnull=True).values_list(
            "taxon_name_id", "id"
        )
    }
    tax_by_scientific = {
        sci.lower(): pk
        for sci, pk in Taxonomy.objects.exclude(scientific_name__isnull=True).values_list(
            "scientific_name", "id"
        )
    }

    batch_size = 2000
    to_update = []
    processed = 0
    matched = 0

    for prev in TaxonPreviousName.objects.all().iterator():
        processed += 1
        prev_tax_id = None

        if prev.previous_name_id and prev.previous_name_id in tax_by_name_id:
            prev_tax_id = tax_by_name_id[prev.previous_name_id]
        elif prev.previous_scientific_name:
            prev_tax_id = tax_by_scientific.get(prev.previous_scientific_name.lower())

        if prev_tax_id:
            prev.previous_taxonomy_id = prev_tax_id
            to_update.append(prev)
            matched += 1

        if processed % batch_size == 0:
            TaxonPreviousName.objects.bulk_update(to_update, ["previous_taxonomy_id"])
            to_update = []
            sys.stdout.write(
                f"populate_previous_taxonomy: processed={processed:,}, matched={matched:,}\n"
            )
            sys.stdout.flush()

    # flush remaining
    if to_update:
        TaxonPreviousName.objects.bulk_update(to_update, ["previous_taxonomy_id"])

    sys.stdout.write(
        f"populate_previous_taxonomy: completed processed={processed:,}, matched={matched:,}\n"
    )
    sys.stdout.flush()

class Migration(migrations.Migration):
    dependencies = [
        ('boranga', '0595_taxonpreviousname_previous_taxonomy_and_more'),
    ]
    operations = [
        migrations.RunPython(populate_previous_taxonomy, reverse_code=migrations.RunPython.noop),
    ]