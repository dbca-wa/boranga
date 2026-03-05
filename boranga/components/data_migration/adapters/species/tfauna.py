import csv
import logging
import os

from django.utils import timezone

from boranga.components.data_migration import mappings as dm_mappings
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
        # lodgement_date: set directly in extract() to the migration datetime
        # (Task 11853: use date of migration rather than NULL)
        # fauna_sub_group_id / fauna_group_id: set directly in extract() via LegacyValueMap
        # (Tasks 11844, 11843: resolved from PhyloGroup column)
        # processing_status is resolved in extract() from the CS column (Task 11854)
        # with a fallback to "historical" when CS is absent/unrecognised.
        # regions_raw: set directly in extract() from the Region column (Task 11872)
    }

    def extract(self, path: str, **options) -> ExtractionResult:
        # Task 11848: Concatenate Category and Notes
        # Task 11851: Concatenate File Nos
        # Task 11843+11844: fauna_group / fauna_sub_group via PhyloGroup
        # Task 11853: lodgement_date = migration datetime
        # Task 11872: regions from Region column

        # We need to map row keys manually or rely on schema.map_raw_row

        raw_rows, warnings = self.read_table(path)

        # Task 11853: capture migration datetime once for the whole batch so
        # every created TFAUNA species carries the same migration timestamp.
        migration_datetime = timezone.now()

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

        # Tasks 11843+11844: preload PhyloGroup → FaunaSubGroup PK mapping via LegacyValueMap.
        dm_mappings.preload_map("TFAUNA", "PhyloGroup")
        phylo_table = dm_mappings._CACHE.get(("TFAUNA", "PhyloGroup"), {})

        # Build FaunaSubGroup.id → fauna_group_id parent map so we can derive
        # fauna_group_id immediately after resolving fauna_sub_group_id.
        sub_group_to_fauna_group: dict[int, int] = {}
        try:
            from boranga.components.species_and_communities.models import FaunaSubGroup

            for sg in FaunaSubGroup.objects.select_related("fauna_group").all():
                if sg.fauna_group_id:
                    sub_group_to_fauna_group[sg.id] = sg.fauna_group_id
            logger.info(
                "TFAUNA: loaded %d FaunaSubGroup→FaunaGroup parent mappings",
                len(sub_group_to_fauna_group),
            )
        except Exception:
            logger.exception("TFAUNA: failed to preload FaunaSubGroup parent map")

        # Task 11854: processing_status — determine from approved ConservationStatus.
        # Pre-load NameID → taxonomy_id from LegacyTaxonomyMapping, then build a
        # set of taxonomy_ids that have a current approved CS.
        from boranga.components.data_migration.registry import _load_legacy_taxonomy_id_mappings

        ltm_table = _load_legacy_taxonomy_id_mappings("TFAUNA")
        # Build NameID → taxonomy_id lookup for quick resolution in the row loop.
        name_id_to_taxonomy_id: dict[str, int] = {}
        for legacy_id, entry in ltm_table.items():
            tid = entry.get("taxonomy_id")
            if tid:
                name_id_to_taxonomy_id[legacy_id] = tid

        # Taxonomy IDs with at least one approved ConservationStatus.
        taxonomy_ids_with_approved_cs: set[int] = set()
        try:
            from boranga.components.conservation_status.models import ConservationStatus

            taxonomy_ids_with_approved_cs = set(
                ConservationStatus.objects.filter(
                    processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED,
                    species_taxonomy_id__isnull=False,
                ).values_list("species_taxonomy_id", flat=True)
            )
            logger.info(
                "TFAUNA: loaded %d taxonomy_ids with an approved ConservationStatus",
                len(taxonomy_ids_with_approved_cs),
            )
        except Exception:
            logger.exception("TFAUNA: failed to preload approved ConservationStatus taxonomy_ids")

        rows = []

        # Tasks 11849+11850: check once whether the Conservation Plan columns
        # are present.  S&C will add them to the CSV; until then we warn once
        # and leave the fields at their defaults (False / None).
        _sample_headers = set(raw_rows[0].keys()) if raw_rows else set()
        has_conservation_plan_col = "Conservation Plan" in _sample_headers
        has_conservation_plan_notes_col = "Conservation Plan Notes" in _sample_headers
        if not has_conservation_plan_col:
            logger.warning(
                "TFAUNA: 'Conservation Plan' column not found in CSV – "
                "conservation_plan_exists will default to False for all rows. "
                "(Task 11849: S&C to add this column.)"
            )
        if not has_conservation_plan_notes_col:
            logger.warning(
                "TFAUNA: 'Conservation Plan Notes' column not found in CSV – "
                "conservation_plan_reference will be empty for all rows. "
                "(Task 11850: S&C to add this column.)"
            )

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

            # Task 11849: conservation_plan_exists
            # Column "Conservation Plan" contains Y/N. Map to boolean.
            if has_conservation_plan_col:
                cp_val = (raw.get("Conservation Plan") or "").strip().upper()
                canonical["conservation_plan_exists"] = cp_val in ("Y", "YES", "TRUE", "1")

            # Task 11850: conservation_plan_reference
            # Column "Conservation Plan Notes" contains free-text reference string.
            if has_conservation_plan_notes_col:
                cp_notes = (raw.get("Conservation Plan Notes") or "").strip()
                if cp_notes:
                    canonical["conservation_plan_reference"] = cp_notes

            # Task 11854: processing_status
            # IF the species has a current Approved ConservationStatus → Active
            # (and all SpeciesPublishingStatus sections → Public).
            # ELSE → Historical (and all SpeciesPublishingStatus sections → Private).
            # Resolve NameID → taxonomy_id, then check the pre-loaded approved CS set.
            resolved_tax_id = name_id_to_taxonomy_id.get(name_id_raw) if name_id_raw else None
            if resolved_tax_id and resolved_tax_id in taxonomy_ids_with_approved_cs:
                canonical["processing_status"] = "active"
            else:
                canonical["processing_status"] = "historical"

            # Task 11853: lodgement_date — use migration date rather than NULL
            # so that active records pass the cross-field validation check
            # (active processing_status requires a non-null lodgement_date).
            canonical["lodgement_date"] = migration_datetime

            # Tasks 11843+11844: fauna_sub_group and fauna_group
            # PhyloGroup column → LegacyValueMap("TFAUNA", "PhyloGroup") → FaunaSubGroup PK.
            # fauna_group is then the parent of the resolved FaunaSubGroup.
            phylo_raw = (raw.get("PhyloGroup") or "").strip()
            if phylo_raw:
                norm = dm_mappings._norm(phylo_raw)
                entry = phylo_table.get(norm)
                if entry and not entry.get("ignored"):
                    sub_group_id = entry.get("target_id")
                    if sub_group_id:
                        try:
                            sub_group_id = int(sub_group_id)
                            canonical["fauna_sub_group_id"] = sub_group_id
                            fauna_group_id = sub_group_to_fauna_group.get(sub_group_id)
                            if fauna_group_id:
                                canonical["fauna_group_id"] = fauna_group_id
                            else:
                                logger.warning(
                                    "TFAUNA: no fauna_group parent found for fauna_sub_group_id=%s (PhyloGroup=%r)",
                                    sub_group_id,
                                    phylo_raw,
                                )
                        except (ValueError, TypeError):
                            logger.warning(
                                "TFAUNA: non-integer target_id for PhyloGroup=%r: %r",
                                phylo_raw,
                                sub_group_id,
                            )
                elif entry and entry.get("ignored"):
                    logger.debug("TFAUNA: PhyloGroup=%r intentionally ignored", phylo_raw)
                else:
                    logger.warning(
                        "TFAUNA: no LegacyValueMap entry for PhyloGroup=%r – "
                        "fauna_sub_group_id will be NULL for SpCode=%s",
                        phylo_raw,
                        sp_code,
                    )

            # Task 11872: regions_raw — comma-separated CALMRegion values from
            # the Region column.  Resolved to Region PKs in the handler via
            # load_legacy_to_pk_map("TFAUNA", "Region").
            region_raw = (raw.get("Region") or "").strip()
            if region_raw:
                canonical["regions_raw"] = region_raw

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
