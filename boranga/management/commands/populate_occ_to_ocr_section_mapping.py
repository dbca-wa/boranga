# filepath: /home/oak/dev/boranga/boranga/management/commands/populate_occ_to_ocr_section_mapping.py
import csv
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from boranga.components.main.models import OccToOcrSectionMapping

CSV_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S%z"


def _parse_bool_y_n(val: str) -> bool | None:
    if val is None:
        return None
    v = val.strip().upper()
    if v == "Y":
        return True
    if v == "N":
        return False
    return None


def _parse_datetime(val: str):
    if not val:
        return None
    try:
        return datetime.strptime(val, CSV_DATETIME_FMT)
    except Exception:
        try:
            # tolerant fallback
            return datetime.fromisoformat(val)
        except Exception:
            return None


class Command(BaseCommand):
    help = "Populate OccToOcrSectionMapping from DRF_POP_SECTION_MAP.csv"

    def add_arguments(self, parser):
        parser.add_argument(
            "csv_path", nargs="?", type=str, help="Path to DRF_POP_SECTION_MAP.csv"
        )
        parser.add_argument(
            "--legacy-system",
            type=str,
            default="TPFL",
            help="Legacy system identifier to store in legacy_system (default: TPFL)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and show actions but do not write to DB",
        )
        parser.add_argument(
            "--update-existing",
            action="store_true",
            help="If mapping exists, update processed/error fields (otherwise skip existing)",
        )
        parser.add_argument(
            "--include-inactive",
            action="store_true",
            help="Include rows with IS_ACTIVE != 'Y' (default: only include active rows)",
        )

    def handle(self, *args, **options):
        BASE_DIR = Path(__file__).resolve().parent
        default_csv_path = BASE_DIR / "legacy_data/TPFL/DRF_POP_SECTION_MAP.csv"

        # options['csv_path'] may be None when omitted; handle safely
        csv_path_opt = options.get("csv_path")
        if csv_path_opt:
            csv_path = Path(csv_path_opt)
        else:
            csv_path = default_csv_path

        legacy_system = options["legacy_system"]
        dry_run = options["dry_run"]
        update_existing = options["update_existing"]
        include_inactive = options["include_inactive"]

        if not csv_path.exists():
            raise CommandError(f"CSV file not found: {csv_path}")

        # mapping from legacy SECT_CODE -> OccToOcrSectionMapping.section value
        SECT_CODE_MAP = {
            "HABITAT": OccToOcrSectionMapping.SECTION_HABITAT_COMPOSITION,
            "LOCATION": OccToOcrSectionMapping.SECTION_LOCATION,
            "PLNT_CNT": OccToOcrSectionMapping.SECTION_PLANT_COUNT,
            "VOUCHER": OccToOcrSectionMapping.SECTION_IDENTIFICATION,
            # Add other legacy codes here if encountered
        }

        total = 0
        created = 0
        updated = 0
        skipped = 0
        unknown_codes = set()

        self.stdout.write(f"Reading CSV: {csv_path} (legacy_system={legacy_system})")
        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            required = {"POP_ID", "SHEETNO", "SECT_CODE"}
            headers = set(reader.fieldnames or [])
            if not required.issubset(headers):
                raise CommandError(
                    f"CSV missing required columns. Found headers: {reader.fieldnames}"
                )

            rows = list(reader)

        self.stdout.write(f"Found {len(rows)} rows. Starting (dry_run={dry_run})...")

        # process rows in a transaction per-row to avoid long transactions & to ensure partial progress if desired
        for row in rows:
            total += 1
            pop_id = (row.get("POP_ID") or "").strip()
            sheetno = (row.get("SHEETNO") or "").strip()
            sect_code_raw = (row.get("SECT_CODE") or "").strip().upper()
            is_active = _parse_bool_y_n(row.get("IS_ACTIVE"))
            deact_date = _parse_datetime(row.get("DEACT_DATE") or "")
            # skip unless active or include_inactive True
            if not include_inactive and is_active is False:
                skipped += 1
                continue

            section = SECT_CODE_MAP.get(sect_code_raw)
            if not section:
                unknown_codes.add(sect_code_raw)
                skipped += 1
                continue

            # canonical keys for uniqueness: legacy_system, occ_migrated_from_id, ocr_migrated_from_id, section
            lookup = {
                "legacy_system": legacy_system,
                "occ_migrated_from_id": str(pop_id),
                "ocr_migrated_from_id": str(sheetno),
                "section": section,
            }

            if dry_run:
                self.stdout.write(
                    f"[DRY] would create/update mapping: {lookup} (IS_ACTIVE={is_active}, DEACT_DATE={deact_date})"
                )
                # do not increment created/updated counts in dry-run
                continue

            # persist
            try:
                with transaction.atomic():
                    obj, created_flag = OccToOcrSectionMapping.objects.get_or_create(
                        **lookup,
                        defaults={"processed": False, "error": ""},
                    )
                    if created_flag:
                        created += 1
                    else:
                        if update_existing:
                            changed = False
                            # reset processed/error so mapping will be picked up again
                            if obj.processed is not False:
                                obj.processed = False
                                changed = True
                            if obj.error != "":
                                obj.error = ""
                                changed = True
                            if changed:
                                obj.processed_at = None
                                obj.save(
                                    update_fields=["processed", "processed_at", "error"]
                                )
                                updated += 1
                            else:
                                skipped += 1
                        else:
                            skipped += 1
            except Exception as exc:
                self.stderr.write(
                    f"Error processing row {total} POP_ID={pop_id} SHEETNO={sheetno} SECT_CODE={sect_code_raw}: {exc}"
                )
                skipped += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Import finished. rows={total} created={created} updated={updated} skipped={skipped}"
            )
        )
        if unknown_codes:
            self.stdout.write(
                self.style.WARNING(
                    "Unknown SECT_CODE values encountered: "
                    + ", ".join(sorted(unknown_codes))
                )
            )
