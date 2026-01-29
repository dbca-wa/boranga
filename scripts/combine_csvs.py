import argparse
import os

import pandas as pd


def combine_csvs(file_paths, output_path):
    """
    Docstring for combine_csvs

    :param file_paths: List of input CSV file paths
    :param output_path: Path for the combined output CSV file

    Example usage:

    python3 scripts/combine_csvs.py \
    private-media/legacy_data/TPFL/DRF_TAXON_CONSV_LISTINGS.csv \
    private-media/legacy_data/TPFL/ADDITIONAL_PROFILES_FROM_OLD_NAMES_OCC_NAMES_Sanitised.csv \
    -o private-media/legacy_data/TPFL/species-profiles-combined.csv

    """

    dfs = []
    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return

        print(f"Reading {file_path}...")
        try:
            # Try reading with default encoding first
            df = pd.read_csv(file_path)
        except UnicodeDecodeError:
            # Fallback to potentially common encoding for legacy files
            print(f"Warning: UnicodeDecodeError for {file_path}, trying 'cp1252'...")
            df = pd.read_csv(file_path, encoding="cp1252")

        dfs.append(df)
        print(f" - {len(df)} rows, {len(df.columns)} columns")

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Combined: {len(combined_df)} rows, {len(combined_df.columns)} columns")

    combined_df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine multiple CSV files into one.")
    parser.add_argument("files", nargs="+", help="Input CSV files")
    parser.add_argument(
        "-o",
        "--output",
        default="combined.csv",
        help="Output CSV file (default: combined.csv)",
    )

    args = parser.parse_args()
    combine_csvs(args.files, args.output)
