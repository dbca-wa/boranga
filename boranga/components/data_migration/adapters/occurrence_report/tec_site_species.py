from boranga.components.data_migration.adapters.base import (
    ExtractionResult,
    SourceAdapter,
)
from boranga.components.data_migration.adapters.sources import Source


class OccurrenceReportTecSiteSpeciesAdapter(SourceAdapter):
    source_key = Source.TEC_SITE_SPECIES.value
    domain = "associated_species"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        raw_rows, warnings = self.read_table(path)

        for raw in raw_rows:
            # Construct dictionary
            site_visit_id = raw.get("SITE_VISIT_ID")
            if not site_visit_id:
                # Without a link to Site Visit, we can't link to Occurrence Report
                continue

            # Taxonomy lookup ID
            taxon_name_id = raw.get("taxon_name_id")

            # Comments
            comments_parts = []
            if raw.get("SSP_NOTES"):
                comments_parts.append(raw["SSP_NOTES"])
            if raw.get("SSP_HEIGHT"):
                comments_parts.append(f"Height: {raw['SSP_HEIGHT']}")
            if raw.get("SSP_COLLECTOR_CODE"):
                comments_parts.append(f"Collector Code: {raw['SSP_COLLECTOR_CODE']}")
            if raw.get("SSP_COLLECTION_NUMBER"):
                comments_parts.append(
                    f"Collector Number: {raw['SSP_COLLECTION_NUMBER']}"
                )

            comments = "; ".join(comments_parts)

            row = {
                "migrated_from_id": site_visit_id,  # Link to ORF
                "taxon_name_id": taxon_name_id,
                "comments": comments,
                # Preserve source info for debugging
                "_source_row": raw,
            }
            rows.append(row)

        return ExtractionResult(rows=rows, warnings=warnings)
