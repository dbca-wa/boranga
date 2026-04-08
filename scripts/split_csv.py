"""
Split a CSV file into equal-sized chunks, preserving the header in each chunk
and handling quoted fields that contain embedded newlines.

Usage:
    python scripts/split_csv.py <input.csv> --chunk-size 5000 --output-dir <dir>

    # Also print the migrate_data commands to run for each chunk:
    python scripts/split_csv.py private-media/legacy_data/TPFL/DRF_RFR_FORMS.csv \\
        --chunk-size 5000 \\
        --output-dir private-media/legacy_data/TPFL/chunks \\
        --commands "occurrence_report_legacy --sources TPFL --seed-history"

The first chunk command should include --wipe-targets; subsequent chunks should not.
"""

import argparse
import csv
import math
import os
import sys


def split_csv(input_path: str, output_dir: str, chunk_size: int) -> list[str]:
    """
    Split *input_path* into chunks of *chunk_size* data rows each.
    Returns list of output file paths in order.
    """
    os.makedirs(output_dir, exist_ok=True)

    input_stem = os.path.splitext(os.path.basename(input_path))[0]

    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            print("ERROR: input file is empty", file=sys.stderr)
            sys.exit(1)

        chunk_index = 0
        row_count = 0
        output_files = []
        current_writer = None
        current_file = None

        def _open_next_chunk():
            nonlocal chunk_index, current_writer, current_file
            if current_file:
                current_file.close()
            chunk_index += 1
            path = os.path.join(output_dir, f"{input_stem}_{chunk_index:04d}.csv")
            output_files.append(path)
            current_file = open(path, "w", newline="", encoding="utf-8")
            current_writer = csv.writer(current_file, quoting=csv.QUOTE_MINIMAL)
            current_writer.writerow(header)
            return current_writer

        _open_next_chunk()

        for row in reader:
            if row_count > 0 and row_count % chunk_size == 0:
                _open_next_chunk()
            current_writer.writerow(row)
            row_count += 1

        if current_file:
            current_file.close()

    print(f"Split {row_count} rows into {chunk_index} chunk(s) of up to {chunk_size} rows each.")
    print(f"Output directory: {output_dir}")
    return output_files


def main():
    parser = argparse.ArgumentParser(description="Split a CSV into equal-sized chunks.")
    parser.add_argument("input", help="Path to the input CSV file.")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of data rows per chunk (default: 5000).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write chunk files into. Defaults to <input_dir>/chunks/.",
    )
    parser.add_argument(
        "--handler",
        default=None,
        metavar="SLUG",
        help="Handler slug, e.g. 'occurrence_report_legacy'. If supplied, print migrate_data commands.",
    )
    parser.add_argument(
        "--handler-args",
        default="",
        metavar="ARGS",
        help="Extra args appended to each command, e.g. '--sources TPFL --seed-history'.",
    )
    args = parser.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_dir = args.output_dir or os.path.join(os.path.dirname(os.path.abspath(input_path)), "chunks")

    output_files = split_csv(input_path, output_dir, args.chunk_size)

    if args.handler and output_files:
        handler = args.handler.strip()
        extra = (" " + args.handler_args.strip()) if args.handler_args.strip() else ""
        print("\n--- Commands to run ---")
        for i, path in enumerate(output_files):
            wipe = " --wipe-targets" if i == 0 else ""
            print(f"./manage.py migrate_data run {handler} {path}{wipe}{extra}")
        total = len(output_files)
        est_seconds_per_chunk = math.ceil(args.chunk_size / 500 * 83)  # ~83 sec/500 rows observed on AKS
        est_minutes_per_chunk = round(est_seconds_per_chunk / 60)
        print(
            f"\nEstimated time per chunk on AKS (contested, excl. startup overhead): ~{est_minutes_per_chunk} min  "
            f"({total} chunks x ~{est_minutes_per_chunk} min = ~{total * est_minutes_per_chunk} min total, "
            f"plus ~2-5 min mapping preload overhead per chunk)"
        )


if __name__ == "__main__":
    main()
