import logging
import os

from boranga.components.data_migration.adapters.base import ExtractionResult, SourceAdapter
from boranga.components.data_migration.adapters.sources import Source

from .tec_shared import build_site_species_comments, load_site_visit_to_sid_mapping

logger = logging.getLogger(__name__)


class OccurrenceReportTecSiteSpeciesAdapter(SourceAdapter):
    source_key = Source.TEC_SITE_SPECIES.value
    domain = "associated_species"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        raw_rows, warnings = self.read_table(path)

        # Load SITE_VISITS.csv to map SITE_VISIT_ID -> S_ID
        site_visits_path = path.replace("SITE_SPECIES.csv", "SITE_VISITS.csv")
        if not os.path.exists(site_visits_path):
            # Try looking in same directory
            site_visits_path = os.path.join(os.path.dirname(path), "SITE_VISITS.csv")

        site_visit_to_sid = load_site_visit_to_sid_mapping(site_visits_path)

        for raw in raw_rows:
            # Get SITE_VISIT_ID and map to S_ID
            site_visit_id = raw.get("SITE_VISIT_ID")
            if not site_visit_id:
                continue

            # Look up S_ID from SITE_VISITS mapping
            s_id = site_visit_to_sid.get(str(site_visit_id))
            if not s_id:
                # Can't link to occurrence report without S_ID
                continue

            # Taxonomy lookup ID (SSP_NAME_ID in TEC CSV)
            taxon_name_id = raw.get("SSP_NAME_ID")

            # Build comments from SSP_ fields
            comments = build_site_species_comments(raw)

            row = {
                "migrated_from_id": f"tec-site-{site_visit_id}",  # Link to OCR via SITE_VISIT_ID
                "taxon_name_id": taxon_name_id,
                "comments": comments,
                # Preserve source info for debugging
                "_source_row": raw,
                "_site_visit_id": site_visit_id,
            }
            rows.append(row)

        return ExtractionResult(rows=rows, warnings=warnings)
