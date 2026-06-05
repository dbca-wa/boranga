import argparse
import os
import sys

import django
import pandas as pd

from boranga.components.species_and_communities.models import Taxonomy

# Add the project root to sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# Set up Django environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "boranga.settings")
django.setup()


def check_taxonomy_mapping(csv_path, column_name):
    if not os.path.exists(csv_path):
        print(f"Error: File not found at {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if column_name not in df.columns:
        print(f"Error: Column '{column_name}' not found in CSV. Available columns: {', '.join(df.columns)}")
        return

    # Get unique values, dropping NaNs
    csv_values = df[column_name].dropna().unique()

    print(f"Checking {len(csv_values)} unique values from column '{column_name}'...")

    results = []
    missing = []
    invalid_format = []

    for val in csv_values:
        try:
            # Taxonomy.taxon_name_id is an IntegerField
            taxon_id = int(float(val))
        except (ValueError, TypeError):
            invalid_format.append(val)
            continue

        exists = Taxonomy.all_objects.filter(taxon_name_id=taxon_id).exists()
        if exists:
            results.append(taxon_id)
        else:
            missing.append(taxon_id)

    print("\nSummary:")
    print(f"  Total unique non-null values: {len(csv_values)}")
    print(f"  Valid Taxon IDs found:         {len(results)}")
    print(f"  Valid Taxon IDs missing:       {len(missing)}")
    if invalid_format:
        print(f"  Invalid formats (non-numeric): {len(invalid_format)}")

    if missing:
        print("\nMissing Taxon IDs:")
        for m in sorted(missing):
            print(f"  - {m}")

    if invalid_format:
        print("\nInvalid Format Values:")
        for inv in sorted(invalid_format):
            print(f"  - {inv}")

    if not missing and not invalid_format:
        print("\nAll values mapped successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if a CSV column maps to Taxonomy.taxon_name_id")
    parser.add_argument("csv_path", help="Path to the .csv file")
    parser.add_argument("column_name", help="Name of the column containing Taxon IDs")

    args = parser.parse_args()

    check_taxonomy_mapping(args.csv_path, args.column_name)
