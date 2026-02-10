from boranga.components.data_migration.adapters.base import ExtractionResult, SourceAdapter
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.mappings import get_group_type_id
from boranga.components.data_migration.registry import (
    static_value_factory,
)


class SpeciesTfaunaAdapter(SourceAdapter):
    source_key = Source.TFAUNA.value
    domain = "species"

    PIPELINES = {
        "group_type": [static_value_factory(get_group_type_id("fauna"))],
        "noo_auto": [static_value_factory(True)],
        "distribution": [static_value_factory(None)],
        "submitter": ["default_user_for_source"],
        "lodgement_date": [static_value_factory(None)],  # or migrate date?
        "processing_status": [
            # If "Condition" (mapped to something?) is TRUE -> Active, False -> Historical
            # Task says: "Mapped Legacy Field: CS [per condition] ... IF CS = TRUE, then processing_status = Active, IF CS=FALSE, then processing_status = Historical"
            # Schema needs to map CS -> processing_status ? or we do it here
        ],
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        # Task 11848: Concatenate Category and Notes
        # Task 11851: Concatenate File Nos

        # We need to map row keys manually or rely on schema.map_raw_row

        raw_rows, warnings = self.read_table(path)
        rows = []

        for raw in raw_rows:
            canonical = {}

            # Task 11857: migrated_from_id = SpCode
            sp_code = raw.get("SpCode")
            if not sp_code:
                # Should we skip?
                pass
            canonical["migrated_from_id"] = str(sp_code) if sp_code else None

            # Task 11848: comment
            category = raw.get("Category", "")
            notes = raw.get("Notes", "")
            # "Category: list match codes to canonical name per list = CategoryList"
            # (Assuming matching happens later or we do it here?)
            # For now concatenation:
            canonical["comment"] = f"{category}. {notes}"

            # Task 11851: department_file_numbers
            file_nos = []
            if raw.get("DBCA File No"):
                file_nos.append(f"DBCA: {raw['DBCA File No']}")
            if raw.get("DPaW File No"):
                file_nos.append(f"DPaW: {raw['DPaW File No']}")
            if raw.get("DEC File No"):
                file_nos.append(f"DEC: {raw['DEC File No']}")
            if raw.get("CALM File No"):
                file_nos.append(f"CALM: {raw['CALM File No']}")
            canonical["department_file_numbers"] = "; ".join(file_nos)

            # Task 11854: processing_status
            # "Mapped Legacy Field: CS"
            cs_val = raw.get("CS")
            if str(cs_val).upper() in ["TRUE", "YES", "1", "T"]:
                canonical["processing_status"] = "active"  # Lowercase for matching choices?
            elif str(cs_val).upper() in ["FALSE", "NO", "0", "F"]:
                canonical["processing_status"] = "historical"
            else:
                # Default?
                pass

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
