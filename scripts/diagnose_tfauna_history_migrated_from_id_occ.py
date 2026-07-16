"""
Diagnose TFAUNA Occurrence reversion history where the migrated_from_id
stored in the Version serialised data does not match the current record value.

Run with:
    python manage.py shell < scripts/diagnose_tfauna_history_migrated_from_id_occ.py

Output: tfauna_history_migrated_from_id_occ_mismatch.csv in the project root.
"""

import csv
import os
import sys

try:
    from django.conf import settings
    from django.contrib.contenttypes.models import ContentType
    from django.db import connection

    from boranga.components.occurrence.models import Occurrence
except Exception as exc:
    print(f"Import failed: {exc}", file=sys.stderr)
    raise

OUTPUT_PATH = os.path.join(settings.BASE_DIR, "tfauna_history_migrated_from_id_occ_mismatch.csv")

occ_ct = ContentType.objects.get_for_model(Occurrence)
print(f"Occurrence content_type_id = {occ_ct.pk}")
print("Running database-side comparison (this should be fast) ...")

# Extract migrated_from_id directly from the JSON blob in Postgres and compare
# to the live column value in a single query — no Python-side JSON parsing needed.
#
# serialized_data is a JSON array:  [{"model": ..., "fields": {"migrated_from_id": "...", ...}}]
# Postgres JSON path:  serialized_data::json -> 0 -> 'fields' ->> 'migrated_from_id'
SQL = """
    SELECT
        occ.id                                                              AS occ_pk,
        occ.occurrence_number,
        occ.migrated_from_id                                               AS current_migrated_from_id,
        (v.serialized_data::json -> 0 -> 'fields' ->> 'migrated_from_id') AS history_migrated_from_id,
        v.id                                                               AS version_pk,
        r.date_created                                                     AS revision_date,
        r.comment                                                          AS revision_comment
    FROM boranga_occurrence occ
    JOIN reversion_version  v ON v.object_id       = occ.id::text
                              AND v.content_type_id = %s
    JOIN reversion_revision r ON r.id              = v.revision_id
    WHERE occ.migrated_from_id ILIKE 'tfauna-%%'
      AND (v.serialized_data::json -> 0 -> 'fields' ->> 'migrated_from_id')
          IS DISTINCT FROM occ.migrated_from_id
    ORDER BY occ.id, r.date_created;
"""

with connection.cursor() as cursor:
    cursor.execute(SQL, [occ_ct.pk])
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

print(f"Query complete. {len(rows):,} mismatch(es) found.")

if not rows:
    print("No mismatches — all TFAUNA Occurrence history entries match current migrated_from_id values.")
    sys.exit(0)

with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)
    writer.writerows(rows)

unique_occs = len({r[0] for r in rows})
print(f"Found {len(rows):,} mismatched version(s) across {unique_occs:,} Occurrence(s).")
print(f"Output written to: {OUTPUT_PATH}")
