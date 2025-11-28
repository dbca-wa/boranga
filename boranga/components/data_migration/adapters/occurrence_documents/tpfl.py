from boranga.components.data_migration.registry import (
    emailuser_by_legacy_username_factory,
    fk_lookup,
)
from boranga.components.occurrence.models import Occurrence
from boranga.components.species_and_communities.models import (
    DocumentCategory,
    DocumentSubCategory,
)

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema

# TPFL-specific transforms and pipelines
OCCURRENCE_ID_TRANSFORM = fk_lookup(
    Occurrence,
    lookup_field="migrated_from_id",
)

UPLOADED_BY_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

PIPELINES = {
    "occurrence_id": ["strip", "required", OCCURRENCE_ID_TRANSFORM],
    "uploaded_by": ["strip", "required", UPLOADED_BY_TRANSFORM],
    "uploaded_date": ["strip", "required", "datetime_iso"],
}


class OccurrenceDocumentTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "occurrence_report_documents"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        try:
            doc_cat = DocumentCategory.objects.get(
                document_category_name="Correspondence"
            )
        except DocumentCategory.DoesNotExist:
            raise ValueError("DocumentCategory 'Correspondence' does not exist")

        try:
            doc_sub_cat = DocumentSubCategory.objects.get(
                document_category_name="TPFL Liaison Record"
            )
        except DocumentSubCategory.DoesNotExist:
            raise ValueError("DocumentSubCategory 'TPFL Liaison Record' does not exist")

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["document_category_id"] = doc_cat.id
            canonical["document_sub_category_id"] = doc_sub_cat.id
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
OccurrenceDocumentTpflAdapter.PIPELINES = PIPELINES
