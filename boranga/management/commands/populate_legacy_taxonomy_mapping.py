import csv

from django.core.management.base import BaseCommand
from django.db import transaction

from boranga.components.main.models import LegacyTaxonomyMapping
from boranga.components.species_and_communities.models import Taxonomy


class Command(BaseCommand):
    """
    Populate LegacyTaxonomyMapping from a CSV file.

        Usage:
            ./manage.py populate_legacy_taxonomy_mapping \
                private-media/legacy_data/TPFL/legacy-species-names-mapped-Nomos-ID-TPFL.csv \
                --dry-run [--list-name TPFL]

    Expected CSV columns (headers case-insensitive):
    - list_name (required unless overridden with `--list-name`)
      - legacy_canonical_name (required)
      - taxon_name_id (required)
      - TAXONID (optional) -> maps to legacy_taxon_name_id

    The command will:
      - fail a row if list_name, legacy_canonical_name, or taxon_name_id are missing
      - filter rows if `--filter-list-name` is provided (using the list_name from CSV)
      - lookup a `Taxonomy` by `taxon_name_id` and fail the row if none found
      - skip rows where an existing mapping is already fully populated
      (taxonomy set, taxon_name_id matches, and legacy_taxon_name_id matches)
      - create new mappings or update existing (when not fully populated)
      - report counts of created/updated/skipped/failed
      - support `--dry-run` to show actions without touching the DB
    """

    help = "Import LegacyTaxonomyMapping rows from CSV (supports --dry-run)"

    def add_arguments(self, parser):
        parser.add_argument("csvfile", type=str)
        parser.add_argument(
            "--list-name",
            dest="list_name",
            type=str,
            help="Optional: override the CSV 'list_name' for all rows",
            default=None,
        )
        parser.add_argument(
            "--filter-list-name",
            dest="filter_list_name",
            type=str,
            help="Optional: only process rows where CSV 'list_name' matches this value",
            default=None,
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--verbose", action="store_true", help="Show details of skipped rows")

    def _get_field(self, row: dict, *keys):
        """Return the first present, non-empty value from row for given candidate keys."""
        for k in keys:
            v = row.get(k)
            if v is not None:
                v = str(v).strip()
                if v != "":
                    return v
        return None

    def handle(self, *args, **options):
        csvfile = options["csvfile"]
        dry_run = options["dry_run"]
        verbose = options["verbose"]
        # If provided, this will be used for every row instead of the CSV `list_name` value
        list_name_override = options.get("list_name")
        filter_list_name = options.get("filter_list_name")

        rows = []
        with open(csvfile, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                rows.append(r)

        created = 0
        updated = 0
        skipped = 0
        failed = 0
        filtered_out = 0

        with transaction.atomic():
            for r in rows:
                csv_list_name = self._get_field(r, "list_name", "list")

                # Filter by list_name if filter is provided
                if filter_list_name and csv_list_name != filter_list_name:
                    filtered_out += 1
                    continue

                # Use the CLI override if provided, otherwise read from CSV
                list_name = list_name_override or csv_list_name
                legacy_name = self._get_field(
                    r,
                    "legacy_canonical_name",
                    "legacy_canonical",
                    "legacy_name",
                    "legacy",
                    "NAME",
                )
                legacy_taxon_name_id = self._get_field(r, "TAXONID", "legacy_taxon_name_id", "taxonid", "TaxonId")
                if legacy_taxon_name_id:
                    legacy_taxon_name_id = legacy_taxon_name_id.replace("WACENSUS:", "")

                taxon_name_id_raw = self._get_field(r, "taxon_name_id", "taxon_id", "taxonnameid", "nomos_taxon_id")

                if not (list_name and legacy_name and taxon_name_id_raw):
                    self.stderr.write(
                        f"Missing required fields in row: list_name={list_name} "
                        f"legacy_canonical_name={legacy_name} taxon_name_id={taxon_name_id_raw}"
                    )
                    failed += 1
                    continue

                try:
                    taxon_name_id = int(taxon_name_id_raw)
                except ValueError:
                    self.stderr.write(f"Invalid taxon_name_id '{taxon_name_id_raw}'; skipping")
                    failed += 1
                    continue

                # Lookup Taxonomy by taxon_name_id
                try:
                    taxonomy = Taxonomy.all_objects.get(taxon_name_id=taxon_name_id)
                except Taxonomy.DoesNotExist:
                    self.stderr.write(
                        f"No Taxonomy found with taxon_name_id={taxon_name_id}; "
                        f"failing row list_name={list_name} legacy_canonical_name={legacy_name}"
                    )
                    failed += 1
                    continue

                # find existing mapping by (list_name, legacy_taxon_name_id, legacy_canonical_name)
                # This allows duplicate legacy_taxon_name_id entries if they have different names
                try:
                    mapping = LegacyTaxonomyMapping.objects.get(
                        list_name=list_name,
                        legacy_taxon_name_id=legacy_taxon_name_id,
                        legacy_canonical_name=legacy_name,
                    )
                    # Determine if fully populated: taxonomy set and taxon_name_id matches
                    if (
                        mapping.taxonomy_id
                        and mapping.taxon_name_id == taxon_name_id
                        and mapping.legacy_canonical_name == legacy_name
                    ):
                        if verbose:
                            self.stdout.write(
                                f"Skipping {legacy_name} ({legacy_taxon_name_id}): "
                                f"DB ID {mapping.taxon_name_id} matches CSV ID {taxon_name_id}"
                            )
                        skipped += 1
                        continue

                    # needs update
                    if dry_run:
                        self.stdout.write(
                            f"[DRY] would update mapping {list_name}:{legacy_taxon_name_id} ({legacy_name}) "
                            f"-> taxon_name_id={taxon_name_id} taxonomy_id={taxonomy.id}"
                        )
                        updated += 1
                        continue

                    mapping.taxon_name_id = taxon_name_id
                    mapping.taxonomy = taxonomy
                    mapping.legacy_canonical_name = legacy_name
                    mapping.save()
                    updated += 1
                except LegacyTaxonomyMapping.DoesNotExist:
                    # create new mapping
                    if dry_run:
                        self.stdout.write(
                            f"[DRY] would create mapping {list_name}:{legacy_name} -> taxon_name_id={taxon_name_id} "
                            f"taxonomy_id={taxonomy.id} legacy_taxon_name_id={legacy_taxon_name_id}"
                        )
                        created += 1
                        continue

                    LegacyTaxonomyMapping.objects.create(
                        list_name=list_name,
                        legacy_canonical_name=legacy_name,
                        taxon_name_id=taxon_name_id,
                        taxonomy=taxonomy,
                        legacy_taxon_name_id=legacy_taxon_name_id,
                    )
                    created += 1
                except LegacyTaxonomyMapping.MultipleObjectsReturned:
                    self.stderr.write(
                        f"Multiple mappings found for list_name={list_name} "
                        f"legacy_taxon_name_id={legacy_taxon_name_id} "
                        f"legacy_canonical_name={legacy_name}; skipping"
                    )
                    failed += 1
                    continue

        self.stdout.write(
            f"created={created} updated={updated} skipped={skipped} failed={failed} "
            f"filtered_out={filtered_out} (dry_run={dry_run})"
        )
