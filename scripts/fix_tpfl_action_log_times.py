#!/usr/bin/env python3
"""
One-off fix: correct TPFL occurrence report action log timestamps.

Root cause
----------
The OccurrenceReportImporter stored TPFL "Edited in TPFL; ..." action log
entries with a `when` value that is 8 hours *earlier* than the correct Perth
time.

The bug was a double-application of the Perth timezone correction:

  1. The DATETIME_ISO_PERTH pipeline correctly converted the source
     MODIFIED_DATE (which carried a bogus "+00:00" label but represented Perth
     wall-clock time) into a proper UTC datetime — e.g. Perth 10:00 → 02:00 UTC.

  2. The action-log handler then *re-parsed* that already-correct UTC datetime
     by stringifying it ("2024-01-01 02:00:00+00:00"), stripping the "+00:00"
     offset again, and re-stamping with Perth — yielding Perth 02:00 instead of
     Perth 10:00.

  3. Django stored Perth 02:00 as 18:00 UTC (previous day), so the frontend
     displayed 02:00 Perth, which is exactly what UTC 02:00 looks like if you
     mistake it for local time — hence the "showing UTC" appearance.

Fix
---
Perth has no DST and is permanently UTC+8.  The stored `when` is therefore
always exactly 8 hours earlier than it should be.  This script adds
timedelta(hours=8) to every affected record.

Affected records are identified by:
    occurrence_report__migrated_from_id__startswith="tpfl-"
    AND what__startswith="Edited in TPFL;"

Usage
-----
    # Dry run — shows counts, no changes written
    python scripts/fix_tpfl_action_log_times.py --dry-run

    # Apply the fix
    python scripts/fix_tpfl_action_log_times.py

The script must be run from the project root with the correct
DJANGO_SETTINGS_MODULE (or via manage.py shell):

    DJANGO_SETTINGS_MODULE=boranga.settings python scripts/fix_tpfl_action_log_times.py

    # Or via manage.py shell (pipe):
    echo "exec(open('scripts/fix_tpfl_action_log_times.py').read())" | ./manage.py shell
"""

import argparse
import os
import sys
from datetime import timedelta

# ---------------------------------------------------------------------------
# Django bootstrap (no-op when already initialised, e.g. inside manage.py shell)
# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so `boranga` is importable when the
# script is run directly (e.g. python3 scripts/fix_tpfl_action_log_times.py).
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    import django
    from django.conf import settings as _dj_settings

    if not _dj_settings.configured:
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "boranga.settings")
        django.setup()
    elif not _dj_settings.DATABASES:
        django.setup()
except RuntimeError:
    # Already set up — running inside manage.py shell
    pass

# ---------------------------------------------------------------------------
# Imports that require Django to be configured
# ---------------------------------------------------------------------------
from django.db import transaction  # noqa: E402

from boranga.components.occurrence.models import OccurrenceReportUserAction  # noqa: E402

BATCH_SIZE = 500
PERTH_OFFSET = timedelta(hours=8)

# Selector for affected records
WHAT_PREFIX = "Edited in TPFL"
MIGRATED_ID_PREFIX = "tpfl-"


def main(dry_run: bool = False) -> None:
    qs = OccurrenceReportUserAction.objects.filter(
        occurrence_report__migrated_from_id__startswith=MIGRATED_ID_PREFIX,
        what__startswith=WHAT_PREFIX,
    ).order_by("id")

    total = qs.count()
    print(f"Found {total} TPFL occurrence report action log record(s) to correct.")

    if total == 0:
        print("Nothing to do.")
        return

    if dry_run:
        # Show a sample of what would change
        sample = qs[:5]
        print("\nDry run — sample of records (showing current → corrected `when`):")
        for ua in sample:
            corrected = ua.when + PERTH_OFFSET
            print(
                f"  id={ua.id}  OCR={ua.occurrence_report_id}  "
                f"current={ua.when.isoformat()}  corrected={corrected.isoformat()}"
            )
        print(
            f"\nDry run complete. {total} record(s) would be updated (+8 hours each). "
            "Re-run without --dry-run to apply."
        )
        return

    updated = 0
    offset = 0

    with transaction.atomic():
        while offset < total:
            batch = list(qs[offset : offset + BATCH_SIZE])
            for ua in batch:
                ua.when = ua.when + PERTH_OFFSET
            OccurrenceReportUserAction.objects.bulk_update(batch, ["when"], batch_size=BATCH_SIZE)
            updated += len(batch)
            offset += BATCH_SIZE
            print(f"  Updated {updated}/{total}...", end="\r")

    print(f"\nDone. Corrected {updated} record(s).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing to the database.",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
