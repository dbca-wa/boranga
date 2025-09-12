from boranga.components.species_and_communities.models import (
    DocumentCategory,
    DocumentSubCategory,
)

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


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
