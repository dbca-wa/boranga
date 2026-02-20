import csv
import logging
import os

from boranga.components.data_migration.adapters.base import ExtractionResult, SourceAdapter
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    static_value_factory,
    taxonomy_lookup_legacy_id_mapping,
)

logger = logging.getLogger(__name__)

# Task 11848: CategoryList – map single-letter category codes from the TFAUNA
# "Species List" CSV to human-readable canonical names.  Populate with real
# names when the authoritative CategoryList is provided by the data custodian.
# Unknown codes are prefixed with "Category: " as a fallback.
TFAUNA_CATEGORY_MAP: dict[str, str] = {
    # code -> canonical name  (update when CategoryList is supplied)
    # "A": "<canonical name for A>",
    # "B": "<canonical name for B>",
    # ...
}


def resolve_category(code: str) -> str:
    """Resolve a TFAUNA category code to its canonical name.

    Returns the mapped canonical name if available, otherwise returns the raw
    code prefixed with 'Category: ' and emits a warning so missing mappings
    are visible in the import log.
    """
    if not code or not code.strip():
        return ""
    code = code.strip()
    canonical = TFAUNA_CATEGORY_MAP.get(code)
    if canonical:
        return canonical
    logger.warning(
        "TFAUNA CategoryList: no canonical name for code %r – using raw value",
        code,
    )
    return f"Category: {code}"


TAXONOMY_TRANSFORM = taxonomy_lookup_legacy_id_mapping("TFAUNA")


class SpeciesTfaunaAdapter(SourceAdapter):
    source_key = Source.TFAUNA.value
    domain = "species"

    PIPELINES = {
        "group_type": [static_value_factory(get_group_type_id("fauna"))],
        "taxonomy_id": ["blank_to_none", "required", TAXONOMY_TRANSFORM],
        "noo_auto": [static_value_factory(True)],
        "distribution": [static_value_factory(None)],
        "submitter": ["default_user_for_source"],
        "lodgement_date": [static_value_factory(None)],  # or migrate date?
        # processing_status is resolved in extract() from the CS column (Task 11854)
        # with a fallback to "historical" when CS is absent/unrecognised.
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        # Task 11848: Concatenate Category and Notes
        # Task 11851: Concatenate File Nos

        # We need to map row keys manually or rely on schema.map_raw_row

        raw_rows, warnings = self.read_table(path)

        # Task 11848: Notes come from a separate "Species Notes.csv" file
        # keyed by SpCode.  Load it into a lookup dict.
        notes_map: dict[str, str] = {}
        notes_path = os.path.join(os.path.dirname(path), "Species Notes.csv")
        if os.path.isfile(notes_path):
            try:
                with open(notes_path, encoding="utf-8") as fh:
                    for row in csv.DictReader(fh):
                        sp = (row.get("SpCode") or "").strip()
                        note = (row.get("Notes") or "").strip()
                        if sp and note:
                            notes_map[sp] = note
                logger.info("TFAUNA: loaded %d notes from %s", len(notes_map), notes_path)
            except Exception as e:
                warnings.append(type(warnings[0])(f"Failed to read Species Notes.csv: {e}") if warnings else None)
                logger.warning("TFAUNA: failed to read %s: %s", notes_path, e)
        else:
            logger.warning(
                "TFAUNA: Species Notes.csv not found at %s – notes will be empty",
                notes_path,
            )

        rows = []

        for raw in raw_rows:
            canonical = {}

            # Task 11857: migrated_from_id = SpCode
            sp_code = raw.get("SpCode")
            if not sp_code:
                # Should we skip?
                pass
            canonical["migrated_from_id"] = str(sp_code) if sp_code else None

            # taxonomy_id: use NameID column (legacy taxon name id).
            # The pipeline TAXONOMY_TRANSFORM resolves this via
            # LegacyTaxonomyMapping to the actual Taxonomy FK.
            name_id_raw = (raw.get("NameID") or "").strip()
            canonical["taxonomy_id"] = name_id_raw if name_id_raw and name_id_raw != "0" else None

            # Task 11848: comment
            # 1a. Resolve Category code via TFAUNA_CATEGORY_MAP (CategoryList)
            # 1b. Notes: plain string from Species Notes.csv (joined by SpCode)
            # 2. Concatenate with ". " separator
            category_raw = raw.get("Category", "")
            category = resolve_category(category_raw)
            notes = notes_map.get(sp_code, "") if sp_code else ""
            parts = [p for p in (category, notes) if p]
            canonical["comment"] = ". ".join(parts)

            # Task 11851: department_file_numbers
            # Column names match the TFAUNA "Species List" CSV headers.
            file_nos = []
            if raw.get("DBCAFileNum"):
                file_nos.append(f"DBCA: {raw['DBCAFileNum']}")
            if raw.get("DPaWFileNum"):
                file_nos.append(f"DPaW: {raw['DPaWFileNum']}")
            if raw.get("DECFileNum"):
                file_nos.append(f"DEC: {raw['DECFileNum']}")
            if raw.get("CALMFileNum"):
                file_nos.append(f"CALM: {raw['CALMFileNum']}")
            canonical["department_file_numbers"] = "; ".join(file_nos)

            # Task 11854: processing_status
            # Mapped Legacy Field: CS.  IF CS = TRUE → Active, IF CS = FALSE → Historical.
            # The CS column may not be present in the provided CSV; default
            # to "historical" when missing or unrecognised (safest assumption).
            cs_val = raw.get("CS", "")
            cs_upper = str(cs_val).strip().upper() if cs_val else ""
            if cs_upper in ("TRUE", "YES", "1", "T"):
                canonical["processing_status"] = "active"
            else:
                # FALSE / NO / 0 / F / missing / unrecognised → historical
                canonical["processing_status"] = "historical"

            # Task 11859, 11863, 11866, 11871, 11874, 11877 are related to child models (SpeciesDistribution etc)
            # The Handler creates the main Species object first.
            # However, the task mentions fields on SpeciesDistribution but says "From Legacy Table: from Boranga Species model".
            # This implies post-creation logic OR creating children in the handler.
            # Usually handler splits data into main + distribution + publishing status etc.

            # Let's populate the canonical dict with what we have.

            canonical["group_type_id"] = get_group_type_id("fauna")
            canonical["noo_auto"] = True
            canonical["distribution"] = None  # Task 11863

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
