#!/usr/bin/env python3
"""
Split the TFAUNA "Fauna Records.csv" into chunks for memory-safe migration runs.

Why sort by SpCode?
  The TFAUNA adapter assigns a sequential occurrence name per SpCode
  (e.g. "MAMALACL 001", "MAMALACL 002", ...) using an in-memory counter that
  resets on every run.  If rows for the same SpCode span two chunks, both runs
  produce "MAMALACL 001", which creates duplicate Occurrence names.
  Sorting by SpCode and splitting only at SpCode boundaries guarantees each
  species' rows are fully processed in a single run, so counters never conflict.

Why are chunks safe to run in sequence?
  Every row has a unique DBNo (migrated_from_id).  After chunk N is imported,
  those migrated_from_ids are in the DB.  When chunk N+1 runs (without
  --wipe-targets), the handler looks up existing OCRs by migrated_from_id and
  updates them rather than creating duplicates.  The first chunk uses
  --wipe-targets to clear any previous partial import.

Usage:
    python scripts/split_tfauna_csv.py \\
        "private-media/legacy_data/TFAUNA/Fauna Records.csv" \\
        [--target-rows 50000] \\
        [--output-dir private-media/legacy_data/TFAUNA/chunks]

    Default output dir: <input_dir>/chunks/
    Default target rows: 50000 (~1-2 GB peak RSS per run based on observed rates)

After running this script, import each chunk in the printed order.
"""

import argparse
import csv
import os
import sys


def split_csv(input_path: str, target_rows: int, output_dir: str, handler_args: str = "") -> None:
    print(f"Reading {input_path} ...")
    with open(input_path, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            print("Error: could not read CSV header", file=sys.stderr)
            sys.exit(1)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    print(f"Read {len(rows):,} rows, {len(fieldnames)} columns.")

    # Sort by SpCode so all rows of a species are contiguous.
    # Secondary sort by DBNo to keep groups ordered for reproducibility.
    rows.sort(key=lambda r: (r.get("SpCode") or "", r.get("DBNo") or ""))

    os.makedirs(output_dir, exist_ok=True)

    chunks: list[list[dict]] = []
    current_chunk: list[dict] = []
    current_spcode: str | None = None

    for row in rows:
        spcode = row.get("SpCode") or ""
        # Flush current chunk when we hit a target AND are at a species boundary
        if current_chunk and len(current_chunk) >= target_rows and spcode != current_spcode:
            chunks.append(current_chunk)
            current_chunk = []
        current_chunk.append(row)
        current_spcode = spcode

    if current_chunk:
        chunks.append(current_chunk)

    # Validate: total rows must be preserved
    total_out = sum(len(c) for c in chunks)
    if total_out != len(rows):
        print(
            f"ERROR: row count mismatch — input={len(rows)}, output={total_out}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\nSplit into {len(chunks)} chunk(s):")

    file_paths: list[str] = []
    for i, chunk in enumerate(chunks, 1):
        out_path = os.path.join(output_dir, f"chunk_{i:03d}.csv")
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(chunk)

        spcodes_in_chunk = len({r.get("SpCode") or "" for r in chunk})
        print(f"  chunk_{i:03d}.csv  {len(chunk):>7,} rows  {spcodes_in_chunk:>4} SpCodes")
        file_paths.append(out_path)

    print(f"\nTotal rows written: {total_out:,}  (matches input: {total_out == len(rows)})")

    # Print copy-paste nohup command
    log_var = "private-media/handler_output/occurrence_report_legacy_$(date +%Y%m%d_%H%M%S).log"
    print("\n" + "=" * 72)
    print("Copy-paste command (chunk 1 wipes, remaining chunks append):")
    print("=" * 72)
    extra_args = (" " + handler_args.strip()) if handler_args.strip() else ""
    lines = []
    for i, path in enumerate(file_paths):
        wipe = " --wipe-targets" if i == 0 else ""
        lines.append(
            f'PYTHONUNBUFFERED=1 ./manage.py migrate_data run occurrence_report_legacy "{path}" --sources TFAUNA{wipe}{extra_args}'
        )
    inner = "\n".join(lines)
    print(
        f"LOG={log_var}\n"
        f"nohup bash -c '\n"
        f"set -euo pipefail\n"
        f"{inner}\n"
        f'\' >"$LOG" 2>&1 &\n'
        f'echo "PID $! Log: tail -f $LOG"'
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input", help='Path to "Fauna Records.csv"')
    parser.add_argument(
        "--target-rows",
        type=int,
        default=50000,
        metavar="N",
        help="Approximate rows per chunk; actual boundary is next SpCode break (default: 50000)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        metavar="DIR",
        help="Directory to write chunk CSVs (default: <input_dir>/chunks/)",
    )
    parser.add_argument(
        "--handler-args",
        default="",
        metavar="ARGS",
        help="Extra args appended to each migrate_data command, e.g. '--seed-history'.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    output_dir = args.output_dir or os.path.join(os.path.dirname(os.path.abspath(args.input)), "chunks")
    split_csv(args.input, args.target_rows, output_dir, args.handler_args)


if __name__ == "__main__":
    main()
