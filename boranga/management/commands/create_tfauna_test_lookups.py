"""
Management command: create_tfauna_test_lookups

Populates all prerequisite lookup data needed to run the TFAUNA species
migration (slug: species_legacy --sources TFAUNA).

Specifically seeds:

  * TFAUNA / PhyloGroup → FaunaSubGroup PKs   (Task 11843+11844)
  * TFAUNA / Region     → Region PKs           (Task 11872)
  * TFAUNA / District   → District PKs         (Task 11875)
  * LegacyTaxonomyMapping (list_name='TFAUNA') – NameID → Taxonomy FK
    for all TFAUNA NameIDs that can be resolved via Taxonomy.taxon_name_id.
    (Task 11847)

All LegacyValueMap entries are idempotent (update_or_create using
legacy_system + list_name + legacy_value as the natural key).
LegacyTaxonomyMapping entries are upserted on (list_name, legacy_taxon_name_id).

Usage:
    ./manage.py create_tfauna_test_lookups
    ./manage.py create_tfauna_test_lookups --wipe          # drop existing entries first
    ./manage.py create_tfauna_test_lookups --species-list <path>  # custom CSV path
    ./manage.py create_tfauna_test_lookups --ocr-test      # also seed OCR-specific test data
    ./manage.py create_tfauna_test_lookups --wipe-ocr-test # remove only the OCR test entries
"""

from __future__ import annotations

import csv
import logging
import os

from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

LEGACY_SYSTEM = "TFAUNA"
DEFAULT_SPECIES_LIST_PATH = "private-media/legacy_data/TFAUNA/Species List.csv"


class Command(BaseCommand):
    help = "Seed TFAUNA lookup data required for the species migration run."

    def add_arguments(self, parser):
        parser.add_argument(
            "--wipe",
            action="store_true",
            help="Delete all existing TFAUNA LegacyValueMap and LegacyTaxonomyMapping entries before seeding.",
        )
        parser.add_argument(
            "--species-list",
            default=DEFAULT_SPECIES_LIST_PATH,
            metavar="PATH",
            help=f"Path to the TFAUNA Species List CSV (default: {DEFAULT_SPECIES_LIST_PATH}).",
        )
        parser.add_argument(
            "--ocr-test",
            action="store_true",
            help=(
                "Also seed OCR-specific test data: SpCode-keyed LegacyTaxonomyMapping entries "
                "(from migrated Species records) and DocumentSubCategory 'Tfauna Document Reference'."
            ),
        )
        parser.add_argument(
            "--wipe-ocr-test",
            action="store_true",
            help="Remove only the OCR test entries added by --ocr-test, then exit.",
        )

    def handle(self, *args, **options):
        from boranga.components.main.models import LegacyTaxonomyMapping, LegacyValueMap
        from boranga.components.species_and_communities.models import (
            District,
            FaunaSubGroup,
            Region,
            Taxonomy,
        )

        # ── --wipe-ocr-test: remove OCR test entries only, then exit ──────────
        if options["wipe_ocr_test"]:
            deleted_ltm, _ = LegacyTaxonomyMapping.objects.filter(
                list_name=LEGACY_SYSTEM,
                legacy_taxon_name_id__startswith="ocr-test-",
            ).delete()
            from boranga.components.occurrence.models import DocumentSubCategory

            deleted_dsc, _ = DocumentSubCategory.objects.filter(
                document_sub_category_name="Tfauna Document Reference"
            ).delete()
            deleted_lvm_res, _ = LegacyValueMap.objects.filter(
                legacy_system=LEGACY_SYSTEM,
                list_name="Resolution",
            ).delete()
            self.stdout.write(
                self.style.WARNING(
                    f"Wiped {deleted_ltm} OCR-test LegacyTaxonomyMapping entries, "
                    f"{deleted_dsc} DocumentSubCategory entry, "
                    f"and {deleted_lvm_res} Resolution LegacyValueMap entries."
                )
            )
            return

        if options["wipe"]:
            deleted_lvm, _ = LegacyValueMap.objects.filter(legacy_system=LEGACY_SYSTEM).delete()
            deleted_ltm, _ = LegacyTaxonomyMapping.objects.filter(list_name=LEGACY_SYSTEM).delete()
            self.stdout.write(
                self.style.WARNING(
                    f"Wiped {deleted_lvm} LegacyValueMap and {deleted_ltm} LegacyTaxonomyMapping entries."
                )
            )

        # ── Content types ──────────────────────────────────────────────────────
        ct_subgroup = ContentType.objects.get_for_model(FaunaSubGroup)
        ct_region = ContentType.objects.get_for_model(Region)
        ct_district = ContentType.objects.get_for_model(District)

        lvm_created = lvm_updated = 0

        def upsert_lvm(list_name, legacy_value, canonical_name, target_ct, target_pk):
            nonlocal lvm_created, lvm_updated
            obj, created = LegacyValueMap.objects.update_or_create(
                legacy_system=LEGACY_SYSTEM,
                list_name=list_name,
                legacy_value=str(legacy_value),
                defaults=dict(
                    canonical_name=canonical_name,
                    target_content_type=target_ct,
                    target_object_id=target_pk,
                    active=True,
                ),
            )
            if created:
                lvm_created += 1
            else:
                lvm_updated += 1

        # ── 1. PhyloGroup → FaunaSubGroup  (Tasks 11843 + 11844) ───────────────
        # Distribution of the 23 observed PhyloGroup codes across the 9 placeholder
        # FaunaSubGroups (3 per FaunaGroup).
        phylo_to_subgroup: dict[str, int] = {
            # code : FaunaSubGroup.id
            "1": 1,
            "2": 1,
            "49": 1,
            "4": 2,
            "5": 2,
            "63": 2,
            "6": 3,
            "9": 3,
            "64": 3,
            "10": 4,
            "11": 4,
            "65": 4,
            "12": 5,
            "31": 5,
            "66": 5,
            "32": 6,
            "33": 6,
            "34": 7,
            "35": 7,
            "36": 8,
            "37": 8,
            "41": 8,
            "38": 9,
        }

        subgroups_by_id = {sg.id: sg for sg in FaunaSubGroup.objects.all()}
        missing_sg = set(phylo_to_subgroup.values()) - set(subgroups_by_id.keys())
        if missing_sg:
            self.stdout.write(
                self.style.ERROR(
                    f"FaunaSubGroup PKs {sorted(missing_sg)} not found – run DefaultDataManager or create them first."
                )
            )
            return

        for code, sg_id in phylo_to_subgroup.items():
            sg = subgroups_by_id[sg_id]
            upsert_lvm("PhyloGroup", code, sg.name, ct_subgroup, sg_id)
        self.stdout.write(f"  PhyloGroup:  seeded {len(phylo_to_subgroup)} entries.")

        # ── 2. Region  (Task 11872) ─────────────────────────────────────────────
        # TFAUNA Region column contains numeric codes "1"–"9" matching Region PKs.
        regions_by_id = {r.id: r for r in Region.objects.all()}
        seeded_regions = 0
        for code in [str(i) for i in range(1, 10)]:
            region = regions_by_id.get(int(code))
            if not region:
                self.stdout.write(self.style.WARNING(f"  Region id={code} not found in DB – skipping."))
                continue
            upsert_lvm("Region", code, region.name, ct_region, region.id)
            seeded_regions += 1
        self.stdout.write(f"  Region:      seeded {seeded_regions} entries.")

        # ── 3. District  (Task 11875) ───────────────────────────────────────────
        # Legacy district values in Species Districts.csv are full district names.
        seeded_districts = 0
        for district in District.objects.select_related("region").all():
            upsert_lvm("District", district.name, district.name, ct_district, district.id)
            seeded_districts += 1
        self.stdout.write(f"  District:    seeded {seeded_districts} entries.")

        self.stdout.write(
            f"  LegacyValueMap total: created={lvm_created} updated={lvm_updated} "
            f"(total TFAUNA: {LegacyValueMap.objects.filter(legacy_system=LEGACY_SYSTEM).count()})"
        )

        # ── 4. LegacyTaxonomyMapping  (Task 11847) ──────────────────────────────
        # Match TFAUNA NameID → Taxonomy via Taxonomy.taxon_name_id.
        species_list_path = options["species_list"]
        if not os.path.isfile(species_list_path):
            self.stdout.write(
                self.style.WARNING(
                    f"  Species List not found at {species_list_path!r} – skipping LegacyTaxonomyMapping seeding."
                )
            )
        else:
            # Read all NameID → ScName pairs from the CSV
            name_id_to_scname: dict[str, str] = {}
            with open(species_list_path, encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    nid = (row.get("NameID") or "").strip()
                    scname = (row.get("ScName") or "").strip()
                    if nid and nid != "0" and scname:
                        name_id_to_scname[nid] = scname

            self.stdout.write(f"  Read {len(name_id_to_scname)} NameID→ScName pairs from Species List.")

            # Bulk-fetch matching Taxonomy objects
            int_ids = {int(nid) for nid in name_id_to_scname}
            taxon_by_name_id: dict[int, Taxonomy] = {
                t.taxon_name_id: t for t in Taxonomy.objects.filter(taxon_name_id__in=int_ids)
            }
            self.stdout.write(f"  Resolved {len(taxon_by_name_id)}/{len(int_ids)} NameIDs via Taxonomy.taxon_name_id.")

            ltm_created = ltm_updated = ltm_skipped = 0
            for nid_str, scname in name_id_to_scname.items():
                taxonomy = taxon_by_name_id.get(int(nid_str))
                if taxonomy is None:
                    ltm_skipped += 1
                    continue
                obj, created = LegacyTaxonomyMapping.objects.update_or_create(
                    list_name=LEGACY_SYSTEM,
                    legacy_taxon_name_id=nid_str,
                    defaults=dict(
                        legacy_canonical_name=scname,
                        taxon_name_id=taxonomy.taxon_name_id,
                        taxonomy=taxonomy,
                    ),
                )
                if created:
                    ltm_created += 1
                else:
                    ltm_updated += 1

            self.stdout.write(
                f"  LegacyTaxonomyMapping: created={ltm_created} updated={ltm_updated} skipped(no match)={ltm_skipped}"
            )

        self.stdout.write(self.style.SUCCESS("\nDone."))

        if options["ocr_test"]:
            self.stdout.write("\nSeeding OCR test data ...")
            self._seed_ocr_test_data()
            self.stdout.write(self.style.SUCCESS("OCR test data seeded. Remove with --wipe-ocr-test when done.\n"))

    def _seed_ocr_test_data(self):
        """Seed OCR-specific test data:
        1. SpCode-keyed LegacyTaxonomyMapping entries built from migrated Species records.
        2. DocumentSubCategory 'Tfauna Document Reference'.
        These are tagged with 'ocr-test-' prefix on legacy_taxon_name_id for easy cleanup.
        """
        from boranga.components.main.models import LegacyTaxonomyMapping, LegacyValueMap
        from boranga.components.occurrence.models import DocumentSubCategory, LocationAccuracy
        from boranga.components.species_and_communities.models import Species

        # 1. SpCode → Species/Taxonomy via migrated_from_id
        # TFAUNA species are stored with migrated_from_id="tfauna-{SpCode}";
        # strip the prefix to get the raw SpCode used in Fauna Records.
        qs = Species.objects.filter(
            migrated_from_id__startswith="tfauna-",
            taxonomy_id__isnull=False,
        ).select_related("taxonomy")

        created = updated = 0
        for species in qs:
            sp_code = species.migrated_from_id[len("tfauna-") :]  # strip "tfauna-" prefix
            taxonomy = species.taxonomy
            obj, was_created = LegacyTaxonomyMapping.objects.update_or_create(
                list_name=LEGACY_SYSTEM,
                legacy_taxon_name_id=f"ocr-test-{sp_code}",
                defaults=dict(
                    legacy_canonical_name=sp_code,
                    taxonomy_id=taxonomy.id,
                    taxon_name_id=taxonomy.taxon_name_id,
                ),
            )
            if was_created:
                created += 1
            else:
                updated += 1

        self.stdout.write(
            f"  OCR test — SpCode LegacyTaxonomyMapping: created={created} updated={updated} "
            f"(from {qs.count()} migrated Species)"
        )

        # 2. DocumentSubCategory 'Tfauna Document Reference'
        from boranga.components.species_and_communities.models import DocumentCategory

        doc_cat = DocumentCategory.objects.filter(document_category_name="ORF Document").first()
        if not doc_cat:
            self.stdout.write(
                self.style.ERROR("  DocumentCategory 'ORF Document' not found — skipping DocumentSubCategory creation.")
            )
            return
        dsc, dsc_created = DocumentSubCategory.objects.get_or_create(
            document_sub_category_name="Tfauna Document Reference",
            defaults={"document_category": doc_cat},
        )
        self.stdout.write(
            f"  OCR test — DocumentSubCategory 'Tfauna Document Reference': "
            f"{'created' if dsc_created else 'already exists'} (pk={dsc.pk})"
        )

        # 3. Resolution → LocationAccuracy LegacyValueMap (Task 12762, provisional)
        # Provisional mapping pending S&C confirmation.
        # Code values from 'Fauna Records.csv' Resolution column.
        RESOLUTION_TO_ACCURACY_PK = {"1": 5, "2": 3, "3": 4, "4": 7, "5": 9, "6": 11}
        ct_la = ContentType.objects.get_for_model(LocationAccuracy)
        lvm_created = lvm_updated = lvm_skipped = 0
        for code, pk in RESOLUTION_TO_ACCURACY_PK.items():
            la = LocationAccuracy.objects.filter(pk=pk).first()
            if la is None:
                self.stdout.write(
                    self.style.WARNING(f"  LocationAccuracy pk={pk} not found — skipping Resolution code '{code}'")
                )
                lvm_skipped += 1
                continue
            _, created = LegacyValueMap.objects.update_or_create(
                legacy_system=LEGACY_SYSTEM,
                list_name="Resolution",
                legacy_value=code,
                defaults=dict(
                    canonical_name=la.name,
                    target_content_type=ct_la,
                    target_object_id=pk,
                    active=True,
                ),
            )
            if created:
                lvm_created += 1
            else:
                lvm_updated += 1
        self.stdout.write(
            f"  OCR test — Resolution LegacyValueMap: created={lvm_created} "
            f"updated={lvm_updated} skipped={lvm_skipped} "
            f"(provisional, pending S\u0026C confirmation for task 12762)"
        )
