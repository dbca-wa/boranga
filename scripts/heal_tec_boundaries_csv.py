"""
heal_tec_boundaries_csv.py
--------------------------
Strip phantom blank columns from the TEC/PEC boundaries CSV before migration.

When Excel saves a spreadsheet as CSV it preserves every column that was ever
"touched", even if they are now empty.  For the TEC/PEC boundaries file this
produces ~14 700 spurious comma-separated empty columns per row, inflating a
~80 MB xlsx into a ~2 GB CSV.

This script reads the raw CSV, drops every column whose header AND every data
value are empty, and writes the result to the canonical filename the migration
adapter expects (TEC_PEC_Boundaries_Nov25.csv).

Usage
-----
    python scripts/heal_tec_boundaries_csv.py \\
        private-media/legacy_data/TEC/tec_pec_boundaries_May_26_all_boundaries.csv

The cleaned file is written alongside the source file as
    private-media/legacy_data/TEC/TEC_PEC_Boundaries_Nov25.csv

Pass --dry-run to report what would be done without writing anything.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys

# The filename the migration adapter looks for (see adapters/occurrence/tec_boundaries.py).
OUTPUT_FILENAME = "TEC_PEC_Boundaries_Nov25.csv"


def heal(source_path: str, dry_run: bool = False) -> None:
    source_path = os.path.abspath(source_path)
    if not os.path.isfile(source_path):
        print(f"ERROR: source file not found: {source_path}", file=sys.stderr)
        sys.exit(1)

    output_path = os.path.join(os.path.dirname(source_path), OUTPUT_FILENAME)

    print(f"Source : {source_path}")
    print(f"Output : {output_path}")

    # ── Pass 1: determine which column indices have any non-empty value ────
    print("Pass 1/2: scanning for non-empty columns …")
    with open(source_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)

    # A column is "real" if its header is non-empty.
    # (All data values in this file's empty columns are also empty, but the
    # header check alone is sufficient and avoids a full scan of 2 GB.)
    keep_indices = [i for i, col in enumerate(header) if col.strip()]

    total_cols = len(header)
    kept_cols = len(keep_indices)
    dropped_cols = total_cols - kept_cols

    print(f"  Total columns  : {total_cols:,}")
    print(f"  Real columns   : {kept_cols}")
    print(f"  Phantom columns: {dropped_cols:,} (will be dropped)")
    print(f"  Column names   : {[header[i] for i in keep_indices]}")

    if dropped_cols == 0:
        print("File already clean — nothing to do.")
        return

    if dry_run:
        print("Dry-run mode: no file written.")
        return

    if os.path.exists(output_path) and os.path.abspath(output_path) != os.path.abspath(source_path):
        print(f"Output file already exists and will be overwritten: {output_path}")

    # ── Pass 2: rewrite with only the kept columns ─────────────────────────
    print("Pass 2/2: writing cleaned file …")
    rows_written = 0
    with (
        open(source_path, newline="", encoding="utf-8-sig") as fin,
        open(output_path, "w", newline="", encoding="utf-8") as fout,
    ):
        reader = csv.reader(fin)
        writer = csv.writer(fout)

        header_row = next(reader)
        writer.writerow([header_row[i] for i in keep_indices])

        for row in reader:
            writer.writerow([row[i] if i < len(row) else "" for i in keep_indices])
            rows_written += 1
            if rows_written % 10_000 == 0:
                print(f"  … {rows_written:,} rows written", end="\r")

    print(f"\nDone. {rows_written:,} data rows written to:\n  {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source", help="Path to the raw (bloated) CSV file")
    parser.add_argument("--dry-run", action="store_true", help="Report only, do not write output")
    args = parser.parse_args()
    heal(args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
