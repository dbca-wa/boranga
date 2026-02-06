import csv
import difflib
import os
import sys

import django

from boranga.components.species_and_communities.models import Taxonomy

# Setup Django environment
# Add the project root to sys.path so we can import boranga
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "boranga.settings")
django.setup()


def find_best_guess(name, threshold=0.85):
    if len(name) < 5:
        return None

    parts = name.split()
    genus = parts[0] if parts else None
    if not genus:
        return None

    # Clean genus if it has markers like "?" or "aff."
    genus_clean = genus.replace("?", "").replace("aff.", "").strip()

    # Try to find candidates sharing the same genus
    candidates = list(
        Taxonomy.objects.filter(is_current=True, genera_name__iexact=genus_clean)
        .values_list("scientific_name", flat=True)
        .order_by("scientific_name")
    )

    if not candidates:
        # Fallback try startswith for the first 4 chars of genus (catches typos in genus name)
        candidates = list(
            Taxonomy.objects.filter(is_current=True, scientific_name__istartswith=genus_clean[:4])
            .values_list("scientific_name", flat=True)
            .order_by("scientific_name")
        )

    if candidates:
        matches = difflib.get_close_matches(name, candidates, n=1, cutoff=threshold)
        if matches:
            return matches[0]

    return None


def main():
    csv_path = (
        "/data/data/projects/boranga/private-media/handler_output/"
        "occurrence_report_legacy_errors_20260122_080616.csv"
    )

    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return

    print(f"Reading unique 'associated_species' names from: {os.path.basename(csv_path)}")

    unique_names = set()
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("column") == "associated_species":
                    raw_val = row.get("raw_value")
                    if raw_val:
                        unique_names.add(raw_val)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if not unique_names:
        print("No 'associated_species' errors found in this CSV.")
        return

    print(f"Found {len(unique_names)} unique unresolved names. Testing fuzzy matching...\n")
    print(f"{'RAW NAME':<45} | {'FUZZY MATCH (BEST GUESS)'}")
    print("-" * 90)

    results = []
    for name in sorted(list(unique_names)):
        guess = find_best_guess(name)
        if guess:
            results.append((name, guess))
            print(f"{name:<45} | {guess}")
        else:
            # Uncomment to see non-matches too
            # print(f"{name:<45} | [No candidate found]")
            pass

    print("\n" + "=" * 90)
    print(f"Summary: Found candidates for {len(results)} out of {len(unique_names)} unique names.")


if __name__ == "__main__":
    main()
