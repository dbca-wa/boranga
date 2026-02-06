#!/usr/bin/env python3
import csv
import sys
from collections import defaultdict
from datetime import datetime


def dedupe(input_path, output_path=None):
    if output_path is None:
        if input_path.endswith(".csv"):
            output_path = input_path.replace(".csv", ".deduped.csv")
        else:
            output_path = input_path + ".deduped.csv"

    counts = defaultdict(int)
    migrated_examples = defaultdict(list)
    first_ts = {}
    last_ts = {}

    with open(input_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (
                row.get("column", "").strip(),
                row.get("level", "").strip(),
                row.get("message", "").strip(),
                row.get("raw_value", "").strip(),
            )
            counts[key] += 1
            mid = row.get("migrated_from_id", "").strip()
            if mid and len(migrated_examples[key]) < 10:
                migrated_examples[key].append(mid)
            ts = row.get("timestamp", "").strip()
            try:
                parsed = datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None
            except Exception:
                parsed = None
            if parsed:
                if key not in first_ts or parsed < first_ts[key]:
                    first_ts[key] = parsed
                if key not in last_ts or parsed > last_ts[key]:
                    last_ts[key] = parsed

    # write output
    fieldnames = [
        "level",
        "column",
        "message",
        "raw_value",
        "count",
        "example_migrated_from_ids",
        "first_seen",
        "last_seen",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as outfh:
        writer = csv.DictWriter(outfh, fieldnames=fieldnames)
        writer.writeheader()

        def sort_key(item):
            key, count = item
            col, level, msg, raw = key
            # Sort order: error, warning, info, then count desc
            level_map = {"error": 0, "warning": 1, "info": 2}
            level_priority = level_map.get(level.lower(), 9)
            return (level_priority, -count, col, msg, raw)

        for key, count in sorted(counts.items(), key=sort_key):
            col, level, msg, raw = key
            examples = ";".join(migrated_examples.get(key, []))
            first = first_ts.get(key).isoformat() if key in first_ts else ""
            last = last_ts.get(key).isoformat() if key in last_ts else ""
            writer.writerow(
                {
                    "level": level,
                    "column": col,
                    "message": msg,
                    "raw_value": raw,
                    "count": count,
                    "example_migrated_from_ids": examples,
                    "first_seen": first,
                    "last_seen": last,
                }
            )

    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: dedupe_errors.py <input-csv> [output-csv]")
        sys.exit(2)
    inp = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else None
    outpath = dedupe(inp, out)
    print(outpath)
