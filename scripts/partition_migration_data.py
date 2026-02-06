import argparse
import importlib
import logging
import os
import sys
from collections import defaultdict
from typing import Any

# Setup Django environment
# These imports must be here to setup Django before importing model/adapter classes
sys.path.append(os.getcwd())
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "boranga.settings")
import django  # noqa: E402

django.setup()

import pandas as pd  # noqa: E402

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

TRIVIAL_PIPELINES = [
    ["strip", "blank_to_none"],
    ["strip"],
    ["blank_to_none"],
]


def load_adapter_class(adapter_path: str):
    """
    Load the adapter class dynamically from a dotted path.
    e.g., 'boranga.components.data_migration.adapters.occurrence_report.tpfl.OccurrenceReportTpflAdapter'
    """
    module_name, class_name = adapter_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        logger.error(f"Could not load adapter '{adapter_path}': {e}")
        sys.exit(1)


def get_interesting_columns(adapter_class: Any, headers: list[str], ignore_list: list[str]) -> dict[str, str]:
    """
    Identify columns that have non-trivial pipelines and are not ignored.
    Returns: Dict[header_name, canonical_key]
    """
    interesting_columns = {}

    # Try to access pipelines and column map
    # Some adapters define PIPELINES on the class, some might use a schema module
    pipelines = getattr(adapter_class, "PIPELINES", {})

    # We need the column map to translate CSV headers to canonical keys (which keys are in PIPELINES)
    # Adapters often import schema, but there isn't a strict interface for exposing it on the Adapter class
    # other than usage in extract method.
    # However, conventionally, they might be close.
    # Let's try to inspect the module of the adapter class for a 'schema' object or 'COLUMN_MAP'.

    adapter_module = sys.modules[adapter_class.__module__]
    column_map = {}

    if hasattr(adapter_module, "schema"):
        schema_module = getattr(adapter_module, "schema")
        column_map = getattr(schema_module, "COLUMN_MAP", {})
    elif hasattr(adapter_module, "COLUMN_MAP"):
        column_map = getattr(adapter_module, "COLUMN_MAP")
    else:
        logger.warning(
            "Could not find COLUMN_MAP or schema module in adapter. "
            "Partitioning will only work if headers match pipeline keys directly, "
            "or we might miss interesting columns."
        )

    for header in headers:
        if header in ignore_list:
            continue

        canonical_key = column_map.get(header, header)
        pipeline = pipelines.get(canonical_key, [])

        # Sort pipeline for comparison if it's a list
        if isinstance(pipeline, list):
            # Check if trivial
            is_trivial = False
            # Check for empty pipeline (passthrough usually) or trivial transforms
            if not pipeline:
                is_trivial = True
            elif pipeline in TRIVIAL_PIPELINES:
                is_trivial = True
            # Also check set equality
            elif set(pipeline) == {"strip", "blank_to_none"}:
                is_trivial = True

            if not is_trivial:
                interesting_columns[header] = canonical_key
        else:
            # If pipeline is not a list (advanced usage?), assume interesting
            interesting_columns[header] = canonical_key

    return interesting_columns


def parse_target(canonical_key: str) -> str:
    if "__" in canonical_key:
        model, field = canonical_key.split("__", 1)
        return f"{model}.{field}"
    return f"Main.{canonical_key}"


def partition_data(
    input_path: str,
    output_path: str,
    adapter_path: str,
    ignore_fields: list[str],
    max_cardinality: int = 50,
    heaviest_first: bool = False,
    report_path: str = None,
):
    logger.info(f"Loading adapter: {adapter_path}")
    adapter_class = load_adapter_class(adapter_path)

    logger.info(f"Reading input file: {input_path}")

    # Read all data first to analyze
    # Using csv.DictReader to handle large files better than pandas if memory is concern,
    # but pandas is easier for unique value extraction. For partitioning script, pandas is likely fine.
    try:
        df = pd.read_csv(input_path, dtype=str).fillna("")
    except Exception as e:
        logger.error(f"Failed to read input CSV: {e}")
        sys.exit(1)

    headers = list(df.columns)
    logger.info(f"Found {len(headers)} columns.")

    # Returns Dict[header, canonical_key]
    interesting_columns_map = get_interesting_columns(adapter_class, headers, ignore_fields)
    interesting_columns = list(interesting_columns_map.keys())

    logger.info(f" identified {len(interesting_columns)} interesting columns: {interesting_columns}")

    if not interesting_columns:
        logger.warning("No interesting columns found. Copying only the first row as sample.")
        df.head(1).to_csv(output_path, index=False)
        return

    # Algorithm: Greedy Set Cover
    # 1. Map each row index to the set of unique (column, value) pairs it covers for interesting columns.

    required_features = set()
    row_features = defaultdict(set)

    logger.info("Analyzing unique values...")

    # Pre-check cardinality to filter out high-cardinality columns from exhaustive coverage
    # max_cardinality passed as argument

    final_columns = []

    for col in interesting_columns:
        unique_count = df[col].nunique()
        if unique_count > max_cardinality:
            logger.warning(
                f"Skipping exhaustive coverage for '{col}': {unique_count} unique values (Limit: {max_cardinality})"
            )
        else:
            final_columns.append(col)

    interesting_columns = final_columns
    logger.info(f"Targeting {len(interesting_columns)} columns for exhaustive coverage.")

    for col in interesting_columns:
        unique_vals = df[col].unique()
        for val in unique_vals:
            feature = (col, val)
            required_features.add(feature)

    logger.info(f"Found {len(required_features)} unique value combinations to cover.")

    # Build map of row_idx -> features it covers
    for idx, row in df.iterrows():
        for col in interesting_columns:
            val = row[col]
            feature = (col, val)
            row_features[idx].add(feature)

    # Efficient Greedy Implementation
    # 1. Map feature -> set of row_indices
    feature_to_rows = defaultdict(set)
    for idx, features in row_features.items():
        for f in features:
            feature_to_rows[f].add(idx)

    covered_features = set()
    selected_indices = []
    selected_rows_info = []  # Store (row_idx, new_features_set) for reporting

    while len(covered_features) < len(required_features):
        # Find the feature that is least covered? Or the row that covers most?
        # Standard greedy set cover: pick set (row) that covers most uncovered elements.

        # Filter candidates? Checking all rows every time is O(N * M) where M is selected set size.
        # N=rows, F=features.

        # Simplified approach for speed:
        # Just iterate features, pick the first row that has that feature, add it, mark all its features covered.
        # This is not minimal, but it guarantees coverage and is fast.
        # But user wants "enough records...". Let's try to be somewhat minimal.

        # Let's limit the pool of candidate rows to those strictly needed for remaining features.

        best_row = -1

        # We only need to check rows that contain at least one uncovered feature.
        # But finding the global maximum is slow.

        # Heuristic: Pick a random remaining feature. Find the row that covers it AND the most other remaining features.
        remaining = list(required_features - covered_features)
        if not remaining:
            break

        target_feature = remaining[0]
        candidate_rows = feature_to_rows[target_feature]

        best_row = -1
        best_row_diff = set()
        max_len = -1

        for ridx in candidate_rows:
            # Calculate what this row would add
            new_feats = row_features[ridx] - covered_features
            if len(new_feats) > max_len:
                max_len = len(new_feats)
                best_row = ridx
                best_row_diff = new_feats

        if best_row != -1:
            selected_indices.append(best_row)
            selected_rows_info.append((best_row, best_row_diff))
            covered_features.update(best_row_diff)
            # Remove covered features from feature_to_rows to speed up next iteration
            # actually not strictly needed if we use 'covered_features' check,
            # but cleaning up might save memory/time
        else:
            # Should not happen if data is consistent
            break

        if len(selected_indices) % 100 == 0:
            logger.info(
                f"Selected {len(selected_indices)} rows, "
                f"covered {len(covered_features)}/{len(required_features)} features."
            )

    logger.info(f"Selected {len(selected_indices)} rows out of {len(df)}.")

    if not heaviest_first:
        # Sort by index to preserve relative order roughly
        selected_rows_info.sort(key=lambda x: x[0])
        selected_indices = [x[0] for x in selected_rows_info]

    result_df = df.iloc[selected_indices]

    logger.info(f"Writing to {output_path}")
    result_df.to_csv(output_path, index=False)

    if report_path:
        logger.info(f"Writing report to {report_path}")
        report_data = []
        for idx, features in selected_rows_info:
            # features is a set of (col_name, value)
            # sort features by col name
            sorted_feats = sorted(list(features), key=lambda x: x[0])
            feat_str = "; ".join([f"{c}={v}" for c, v in sorted_feats])
            columns_str = ", ".join(sorted([c for c, v in features]))

            # Build target string
            targets = []
            for col in sorted([c for c, v in features]):
                canonical = interesting_columns_map.get(col, col)
                targets.append(parse_target(canonical))
            targets_str = ", ".join(targets)

            # Get row identity from first few columns (usually ID/Ref)
            original_row = df.loc[idx]
            identity_parts = [f"{h}={original_row[h]}" for h in headers[:3]]
            identity_str = "; ".join(identity_parts)

            row_data = {
                "original_row_index": idx,
                "row_identity": identity_str,
                "new_features_count": len(features),
                "contributing_columns": columns_str,
                "targets": targets_str,
                "new_features": feat_str,
            }
            report_data.append(row_data)

        pd.DataFrame(report_data).to_csv(report_path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Partition migration data for equivalence partitioning")
    parser.add_argument("--adapter", required=True, help="Dotted path to Adapter class")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", required=True, help="Path to output CSV")
    parser.add_argument("--ignore", help="Comma separated list of columns to ignore", default="")
    parser.add_argument(
        "--max-cardinality",
        type=int,
        default=50,
        help="Max unique values per column to attempt coverage for",
    )
    parser.add_argument(
        "--heaviest-first",
        action="store_true",
        help="Sort output by number of new features covered (descending)",
    )

    parser.add_argument(
        "--report",
        help="Path to report CSV detailing features covered per row",
    )

    args = parser.parse_args()

    ignore_list = [x.strip() for x in args.ignore.split(",") if x.strip()]

    partition_data(
        args.input,
        args.output,
        args.adapter,
        ignore_list,
        args.max_cardinality,
        args.heaviest_first,
        args.report,
    )
