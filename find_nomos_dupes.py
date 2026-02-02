#!/usr/bin/env python3
# Stream a JSON file and report duplicate canonical_name keys.
# By default groups by canonical_name only. Use --by-kingdom to group by (canonical_name, kingdom_id).
# Can export results to CSV with --csv OUTPUT.csv

import argparse
import csv
import json
import sys
from collections import defaultdict


def iter_records(path):
    try:
        import ijson
    except Exception:
        ijson = None

    with open(path, "rb") as fh:
        first = fh.read(1)
        if not first:
            return
        fh.seek(0)
        if first == b"[" and ijson:
            # streaming parse of a JSON array
            yield from ijson.items(fh, "item")
            return
    # Fallback: treat as NDJSON (one JSON object per line)
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # if a line fails, try to recover by ignoring it
                continue


def find_duplicates(path, by_kingdom=False, sample_limit=5, csv_path=None):
    # stats: key -> {"count": int, "sample_ids": [..], "kingdoms": set()}
    stats = defaultdict(lambda: {"count": 0, "sample_ids": [], "kingdoms": set()})
    for obj in iter_records(path):
        name = obj.get("canonical_name")
        if name is None:
            continue
        kingdom = obj.get("kingdom_id")
        key = (name, kingdom) if by_kingdom else name
        stats[key]["count"] += 1
        if len(stats[key]["sample_ids"]) < sample_limit:
            stats[key]["sample_ids"].append(obj.get("taxon_name_id"))
        stats[key]["kingdoms"].add(kingdom)

    # prepare results
    rows = []
    for key, info in sorted(stats.items(), key=lambda kv: (-kv[1]["count"], str(kv[0]))):
        if info["count"] > 1:
            if by_kingdom:
                name, kingdom = key
                rows.append(
                    {
                        "canonical_name": name,
                        "kingdom_id": kingdom,
                        "count": info["count"],
                        "sample_taxon_name_ids": ";".join([str(x) for x in info["sample_ids"]]),
                    }
                )
            else:
                rows.append(
                    {
                        "canonical_name": key,
                        "kingdom_id": ";".join(
                            [str(k) for k in sorted(info["kingdoms"], key=lambda x: (x is None, x))]
                        ),
                        "count": info["count"],
                        "sample_taxon_name_ids": ";".join([str(x) for x in info["sample_ids"]]),
                    }
                )

    # print to stdout
    if rows:
        for r in rows:
            print(
                f"Duplicate: canonical_name={r['canonical_name']!r} kingdom_id={r['kingdom_id']!r} "
                f"count={r['count']} sample_taxon_name_ids=[{r['sample_taxon_name_ids']}]"
            )
    else:
        print("No duplicates found.")

    # write CSV if requested
    if csv_path:
        fieldnames = ["canonical_name", "kingdom_id", "count", "sample_taxon_name_ids"]
        with open(csv_path, "w", newline="", encoding="utf-8") as csvf:
            writer = csv.DictWriter(csvf, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    return 0 if not rows else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description="Find duplicate canonical_name entries in a JSON/NDJSON file.")
    parser.add_argument("path", help="Path to JSON file (array or NDJSON).")
    parser.add_argument(
        "-k",
        "--by-kingdom",
        action="store_true",
        help="Group by (canonical_name, kingdom_id) instead of canonical_name only.",
    )
    parser.add_argument(
        "-s",
        "--sample-limit",
        type=int,
        default=5,
        help="How many sample taxon_name_id to show per key.",
    )
    parser.add_argument("--csv", dest="csv_path", help="Write results to CSV file.")
    args = parser.parse_args(argv)

    rc = find_duplicates(
        args.path,
        by_kingdom=args.by_kingdom,
        sample_limit=args.sample_limit,
        csv_path=args.csv_path,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
