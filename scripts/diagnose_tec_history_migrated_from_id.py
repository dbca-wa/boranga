"""
Diagnose TEC/community OccurrenceReport reversion history where the
migrated_from_id stored in the Version serialised data does not match
the current record value.

This is the same check as diagnose_tfauna_history_migrated_from_id.py but
scoped to records whose migrated_from_id begins with 'tec-'.  Run it before
applying the stale-version fix in prod to estimate how many Version rows would
be deleted.

Run with:
    python manage.py shell < scripts/diagnose_tec_history_migrated_from_id.py

Output: tec_history_migrated_from_id_mismatch.csv in the project root.

Summary lines printed to stdout:
  - total mismatch Version rows  (what _clear_stale_migrated_from_id_versions would delete)
  - distinct OCR PKs affected    (how many OCRs would be re-seeded)
"""

import csv
import os
import sys

try:
    from django.conf import settings
    from django.contrib.contenttypes.models import ContentType
    from django.db import connection

    from boranga.components.occurrence.models import OccurrenceReport
except Exception as exc:
    print(f"Import failed: {exc}", file=sys.stderr)
    raise

OUTPUT_PATH = os.path.join(settings.BASE_DIR, "tec_history_migrated_from_id_mismatch.csv")

ocr_ct = ContentType.objects.get_for_model(OccurrenceReport)
print(f"OccurrenceReport content_type_id = {ocr_ct.pk}")
print("Running database-side comparison (this should be fast) ...")

# Extract migrated_from_id directly from the JSON blob in Postgres and compare
# to the live column value in a single query — no Python-side JSON parsing needed.
#
# serialized_data is a JSON array:  [{"model": ..., "fields": {"migrated_from_id": "...", ...}}]
# Postgres JSON path:  serialized_data::json -> 0 -> 'fields' ->> 'migrated_from_id'
SQL = """
    SELECT
        ocr.id                                                              AS ocr_pk,
        ocr.occurrence_report_number,
        ocr.migrated_from_id                                               AS current_migrated_from_id,
        (v.serialized_data::json -> 0 -> 'fields' ->> 'migrated_from_id') AS history_migrated_from_id,
        v.id                                                               AS version_pk,
        r.date_created                                                     AS revision_date,
        r.comment                                                          AS revision_comment
    FROM boranga_occurrencereport ocr
    JOIN reversion_version  v ON v.object_id       = ocr.id::text
                              AND v.content_type_id = %s
    JOIN reversion_revision r ON r.id              = v.revision_id
    WHERE ocr.migrated_from_id ILIKE 'tec-%%'
      AND (v.serialized_data::json -> 0 -> 'fields' ->> 'migrated_from_id')
          IS DISTINCT FROM ocr.migrated_from_id
    ORDER BY ocr.id, r.date_created;
"""

with connection.cursor() as cursor:
    cursor.execute(SQL, [ocr_ct.pk])
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

print(f"Query complete. {len(rows):,} mismatch(es) found.")

if not rows:
    print("No mismatches — all TEC OCR history entries match current migrated_from_id values.")
    sys.exit(0)

with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)
    writer.writerows(rows)

unique_ocrs = len({r[0] for r in rows})
# Each stale OCR PK may have multiple Version rows (one per follow-relation in
# the reversion.register() follow list).  _clear_stale_migrated_from_id_versions
# deletes ALL Version rows for stale PKs, so the total version count below is
# the upper bound on rows that would be deleted if the fix runs in prod.
print(f"Found {len(rows):,} mismatched version(s) across {unique_ocrs:,} OCR(s).")
print(
    f"If the seeder stale-fix runs in prod, up to {len(rows):,} Version rows would be "
    f"deleted and {unique_ocrs:,} OCR(s) re-seeded with correct history."
)
print(f"Output written to: {OUTPUT_PATH}")
