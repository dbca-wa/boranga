"""partition_tec_occurrences.py
───────────────────────────────────────────────────────────────────────────────
Pre- (or post-) import TEC occurrence partitioning tool.

Because TEC stores its data across several related CSVs rather than one flat
file, the generic ``partition_migration_data.py`` cannot be used directly
(it expects a single file and needs Django to load an adapter).

This script is standalone (no Django dependency) and works entirely from the
raw CSV files in the TEC data directory.

What it does
------------
1. Reads the TEC data directory (OCCURRENCES.csv + related tables).
2. For every OCC_UNIQUE_ID computes a *richness score* counting how much
   auxiliary data each occurrence has across all related tables:
     - site_count         (SITES.csv — 1:many on OCC_UNIQUE_ID)
     - fire_count         (FIRE_HISTORY.csv)
     - additional_count   (ADDITIONAL_DATA.csv)
     - species_count      (OCCURRENCE_SPECIES_COMBINED.csv)
     - survey_count       (SURVEYS.csv)
     - threat_count       (SURVEY_THREATS.csv, counted per OCC_UNIQUE_ID)
     - site_visit_count   (SITE_VISITS.csv, resolved via SITES.S_ID)
3. Runs the same greedy set-cover algorithm used in
   ``partition_migration_data.py`` over a curated list of "interesting"
   columns drawn from OCCURRENCES and the first SITES row per occurrence.
   These are the columns that map to non-trivial targets in the adapter
   (FK lookups, complex transforms, habitat fields etc.).
4. Unions the set-cover result with the top-N richest occurrences so that
   the "phattest" records are always included.
5. Writes two output files:
     --output   Filtered OCCURRENCES rows (with extra _richness_score,
                _site_count … columns) ready for business-user verification.
     --report   Full ranking of all occurrences by richness score (descending)
                so verifiers can pick individual records of interest.

Usage
-----
python3 scripts/partition_tec_occurrences.py \\
    --input  private-media/legacy_data/TEC/ \\
    --output private-media/legacy_data/TEC/tec-occurrences-partitioned.csv \\
    --report private-media/legacy_data/TEC/tec-occurrences-report.csv \\
    [--max-cardinality 50] \\
    [--top-n 100] \\
    [--heaviest-last]
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Interesting column definitions
# ──────────────────────────────────────────────────────────────────────────────

# Columns from OCCURRENCES.csv that carry meaningful semantic variety worth
# covering — i.e. they map to non-trivial adapter targets (FK lookups,
# complex transforms, or fields that feed into comment/habitat aggregations).
INTERESTING_OCC_COLUMNS = [
    "OCC_SOURCE_CODE",  # → coordinate_source via LegacyValueMap
    "OCC_BR_CODE",  # → boundary reliability via LegacyValueMap
    "OCC_STATUS_CODE",  # → identification certainty via LegacyValueMap
    "OCC_CTRC_SYS",
    "OCC_CTRC_RECOMMENDATION",
    "OCC_CWEALTH_LISTING",
    "OCC_SOIL",  # → habitat_notes (aggregated)
    "OCC_SURF_GEOLOGY",  # → habitat_notes
    "OCC_LAND_ELEMENT",  # → habitat_notes
    "OCC_WATER",  # → OCCHabitatComposition.water_quality
    "OCC_DRAINAGE",  # → habitat_notes
    "OCC_CLASSIFICATION",  # → habitat_notes
    "OCC_COM_STRUCTURE",  # → OCCVegetationStructure.vegetation_structure_layer_one
    "OCC_ZONE_CODE",
    "OCC_NO_BOUNDARY",
    "OCC_CONFIDENTIAL",
    "identification_comment",  # → OCCIdentification.identification_comment
    "OCC_BEARD_MAP_CODE",  # → comment
    "OCC_BUSH_FOREVER_SITE_NO",  # → comment
]

# Columns from the *first* SITES row per occurrence that add further variety.
INTERESTING_SITE_COLUMNS = [
    "S_DATUM_CODE",
    "S_PERMANENTLY_MARKED",
    "S_ROCK_TYPE",
    "S_PARENT_MATERIAL",
    "S_HABITAT",
    "S_VEGETATION",
]

# Richness component weights.  Adjust to taste — higher weight = more
# influence on which records end up in the top-N selection.
RICHNESS_WEIGHTS = {
    "site_count": 1,
    "fire_count": 3,  # fire history entries are high-value data
    "additional_count": 2,
    "species_count": 2,
    "survey_count": 3,
    "threat_count": 2,
    "site_visit_count": 4,  # site visits are the rarest / most complex
}

RICHNESS_COUNT_COLUMNS = list(RICHNESS_WEIGHTS.keys())

# Columns written to the ranking report (subset of the enriched row).
REPORT_COLUMNS = [
    "OCC_UNIQUE_ID",
    "COM_NO",
    "OCC_NO",
    "_richness_score",
    "_site_count",
    "_fire_count",
    "_additional_count",
    "_species_count",
    "_survey_count",
    "_threat_count",
    "_site_visit_count",
    "OCC_SOURCE_CODE",
    "OCC_BR_CODE",
    "OCC_STATUS_CODE",
    "OCC_SOIL",
    "OCC_SURF_GEOLOGY",
    "OCC_WATER",
    "OCC_DRAINAGE",
    "OCC_COM_STRUCTURE",
    "OCC_CLASSIFICATION",
    "OCC_ZONE_CODE",
    "OCC_BUSH_FOREVER_SITE_NO",
    "identification_comment",
]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def read_csv(path: str | None) -> list[dict]:
    """Read *path* into a list of dicts. Returns [] when path is None or missing."""
    if not path or not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def find_file(directory: str, *names: str) -> str | None:
    """Return the first path that exists from *names* inside *directory*."""
    for name in names:
        p = os.path.join(directory, name)
        if os.path.exists(p):
            return p
    return None


def compute_richness(row: dict, counts: dict) -> int:
    """
    Weighted richness score for a single occurrence.

    = sum(count * weight for related-table counts)
    + 1 for each non-empty interesting OCC column
    """
    score = 0
    for col, weight in RICHNESS_WEIGHTS.items():
        score += counts.get(col, 0) * weight
    for col in INTERESTING_OCC_COLUMNS:
        if row.get(col, "").strip():
            score += 1
    return score


# ──────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ──────────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input",
        required=True,
        help="TEC data directory (must contain OCCURRENCES.csv and related files)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path for the partitioned OCCURRENCES output CSV",
    )
    parser.add_argument(
        "--report",
        required=True,
        help="Path for the full richness-ranking report CSV (all occurrences, desc by score)",
    )
    parser.add_argument(
        "--max-cardinality",
        type=int,
        default=50,
        help=("Skip exhaustive set-cover for columns with more unique values than this (default: 50)"),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Always include the top-N richest occurrences regardless of set-cover (default: 100)",
    )
    parser.add_argument(
        "--heaviest-last",
        action="store_true",
        help="Sort output with heaviest (highest-score) occurrences last (default: heaviest first)",
    )
    return parser.parse_args()


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main() -> None:
    args = parse_args()
    data_dir = args.input

    if not os.path.isdir(data_dir):
        logger.error("--input must be a directory: %s", data_dir)
        sys.exit(1)

    # ── Load primary table ────────────────────────────────────────────────────

    occ_path = find_file(data_dir, "OCCURRENCES.csv", "occurrences.csv")
    if not occ_path:
        logger.error("OCCURRENCES.csv not found in %s", data_dir)
        sys.exit(1)

    logger.info("Loading OCCURRENCES.csv …")
    occ_rows = read_csv(occ_path)
    logger.info("  %s occurrence rows", f"{len(occ_rows):,}")

    # ── Load related tables, index by OCC_UNIQUE_ID ───────────────────────────

    logger.info("Loading SITES.csv …")
    sites_by_occ: dict[str, list[dict]] = defaultdict(list)
    for r in read_csv(find_file(data_dir, "SITES.csv", "sites.csv")):
        sites_by_occ[r.get("OCC_UNIQUE_ID", "")].append(r)
    logger.info("  %s site rows", f"{sum(len(v) for v in sites_by_occ.values()):,}")

    logger.info("Loading FIRE_HISTORY.csv …")
    fire_by_occ: dict[str, list[dict]] = defaultdict(list)
    for r in read_csv(find_file(data_dir, "FIRE_HISTORY.csv", "fire_history.csv")):
        fire_by_occ[r.get("OCC_UNIQUE_ID", "")].append(r)
    logger.info("  %s fire history rows", f"{sum(len(v) for v in fire_by_occ.values()):,}")

    logger.info("Loading ADDITIONAL_DATA.csv …")
    additional_by_occ: dict[str, list[dict]] = defaultdict(list)
    for r in read_csv(find_file(data_dir, "ADDITIONAL_DATA.csv", "additional_data.csv")):
        additional_by_occ[r.get("OCC_UNIQUE_ID", "")].append(r)
    logger.info("  %s additional data rows", f"{sum(len(v) for v in additional_by_occ.values()):,}")

    logger.info("Loading OCCURRENCE_SPECIES_COMBINED.csv …")
    species_by_occ: dict[str, list[dict]] = defaultdict(list)
    for r in read_csv(find_file(data_dir, "OCCURRENCE_SPECIES_COMBINED.csv")):
        species_by_occ[r.get("OCC_UNIQUE_ID", "")].append(r)
    logger.info("  %s associated species rows", f"{sum(len(v) for v in species_by_occ.values()):,}")

    logger.info("Loading SURVEYS.csv …")
    surveys_by_occ: dict[str, list[dict]] = defaultdict(list)
    for r in read_csv(find_file(data_dir, "SURVEYS.csv", "surveys.csv")):
        surveys_by_occ[r.get("OCC_UNIQUE_ID", "")].append(r)
    logger.info("  %s survey rows", f"{sum(len(v) for v in surveys_by_occ.values()):,}")

    logger.info("Loading SURVEY_THREATS.csv …")
    # SURVEY_THREATS has OCC_UNIQUE_ID directly — count per occurrence
    threats_by_occ: dict[str, int] = defaultdict(int)
    for r in read_csv(find_file(data_dir, "SURVEY_THREATS.csv", "survey_threats.csv")):
        threats_by_occ[r.get("OCC_UNIQUE_ID", "")] += 1
    logger.info("  %s threat rows", f"{sum(threats_by_occ.values()):,}")

    # SITE_VISITS is keyed on S_ID; resolve to OCC_UNIQUE_ID via SITES
    logger.info("Loading SITE_VISITS.csv …")
    sid_to_occ: dict[str, str] = {}
    for occ_id, slist in sites_by_occ.items():
        for s in slist:
            sid = s.get("S_ID", "")
            if sid:
                sid_to_occ[sid] = occ_id

    site_visits_by_occ: dict[str, int] = defaultdict(int)
    site_visits_raw = read_csv(find_file(data_dir, "SITE_VISITS.csv", "site_visits.csv"))
    for r in site_visits_raw:
        occ_id = sid_to_occ.get(r.get("S_ID", ""))
        if occ_id:
            site_visits_by_occ[occ_id] += 1
    logger.info("  %s site visit rows", f"{len(site_visits_raw):,}")

    # ── Enrich each occurrence row ────────────────────────────────────────────

    logger.info("Computing richness scores …")
    enriched: list[tuple[dict, dict]] = []

    for row in occ_rows:
        occ_id = row.get("OCC_UNIQUE_ID", "")

        counts = {
            "site_count": len(sites_by_occ.get(occ_id, [])),
            "fire_count": len(fire_by_occ.get(occ_id, [])),
            "additional_count": len(additional_by_occ.get(occ_id, [])),
            "species_count": len(species_by_occ.get(occ_id, [])),
            "survey_count": len(surveys_by_occ.get(occ_id, [])),
            "threat_count": threats_by_occ.get(occ_id, 0),
            "site_visit_count": site_visits_by_occ.get(occ_id, 0),
        }
        counts["richness_score"] = compute_richness(row, counts)

        # Inline richness columns into the row (prefixed with _)
        for k, v in counts.items():
            row[f"_{k}"] = v

        # Flatten interesting columns from the first SITE row
        first_site = (sites_by_occ.get(occ_id) or [{}])[0]
        for col in INTERESTING_SITE_COLUMNS:
            row[f"_site_{col}"] = first_site.get(col, "")

        enriched.append((row, counts))

    # ── Greedy set-cover ──────────────────────────────────────────────────────

    logger.info("Running greedy set-cover over interesting columns …")

    sample_row = enriched[0][0] if enriched else {}

    # Build candidate column list — only columns that actually exist in the data
    site_col_keys = [f"_site_{c}" for c in INTERESTING_SITE_COLUMNS]
    candidate_cols = [c for c in (INTERESTING_OCC_COLUMNS + site_col_keys) if c in sample_row]

    # Drop high-cardinality columns to keep set-cover tractable
    cover_cols: list[str] = []
    for col in candidate_cols:
        unique_vals = {row.get(col, "") for row, _ in enriched}
        if len(unique_vals) <= args.max_cardinality:
            cover_cols.append(col)
        else:
            logger.warning(
                "  Skipping '%s': %d unique values > --max-cardinality=%d",
                col,
                len(unique_vals),
                args.max_cardinality,
            )

    logger.info("  Targeting %d columns for exhaustive coverage", len(cover_cols))

    # Map feature → set of row indices that cover it
    feature_to_rows: dict[tuple, set[int]] = defaultdict(set)
    row_features: dict[int, set[tuple]] = {}

    for idx, (row, _) in enumerate(enriched):
        feats: set[tuple] = set()
        for col in cover_cols:
            feat = (col, row.get(col, ""))
            feature_to_rows[feat].add(idx)
            feats.add(feat)
        row_features[idx] = feats

    required_features: set[tuple] = set(feature_to_rows.keys())
    covered: set[tuple] = set()
    selected_idx: set[int] = set()

    while covered < required_features:
        # Pick the row that covers the most currently uncovered features
        best_idx = max(
            (i for i in range(len(enriched)) if i not in selected_idx),
            key=lambda i: len(row_features[i] - covered),
            default=None,
        )
        if best_idx is None:
            break
        new_cover = row_features[best_idx] - covered
        if not new_cover:
            break
        covered |= new_cover
        selected_idx.add(best_idx)

    logger.info(
        "  Set-cover selected %s occurrences covering %s/%s features",
        f"{len(selected_idx):,}",
        f"{len(covered):,}",
        f"{len(required_features):,}",
    )

    # ── Union with top-N richest ───────────────────────────────────────────────

    sorted_by_richness = sorted(
        range(len(enriched)),
        key=lambda i: enriched[i][1]["richness_score"],
        reverse=True,
    )
    top_n_idx = set(sorted_by_richness[: args.top_n])
    selected_idx |= top_n_idx

    logger.info(
        "  After adding top-%d richest: %s total occurrences selected",
        args.top_n,
        f"{len(selected_idx):,}",
    )

    # ── Write partitioned output ──────────────────────────────────────────────

    selected_rows = [enriched[i][0] for i in selected_idx]
    selected_rows.sort(
        key=lambda r: r.get("_richness_score", 0),
        reverse=not args.heaviest_last,
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    if selected_rows:
        out_fieldnames = list(selected_rows[0].keys())
        with open(args.output, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=out_fieldnames)
            writer.writeheader()
            writer.writerows(selected_rows)
        logger.info("Partitioned output → %s  (%s rows)", args.output, f"{len(selected_rows):,}")

    # ── Write richness report (all occurrences, desc by score) ───────────────

    all_rows_sorted = [enriched[i][0] for i in sorted_by_richness]
    report_fields = [c for c in REPORT_COLUMNS if c in (all_rows_sorted[0] if all_rows_sorted else {})]

    with open(args.report, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=report_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows_sorted)

    logger.info(
        "Richness report    → %s  (%s rows, sorted by richness_score desc)",
        args.report,
        f"{len(all_rows_sorted):,}",
    )


if __name__ == "__main__":
    main()
