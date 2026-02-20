"""
TEC Occurrence Report Document Adapter

Tasks 12502-12508: Import documents from SITE_VISITS.SV_PHOTO field
"""

from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    _result,
    fk_lookup_static,
    static_value_factory,
)
from boranga.components.occurrence.models import OccurrenceReport
from boranga.components.species_and_communities.models import (
    DocumentCategory,
    DocumentSubCategory,
)

from ..base import ExtractionResult, SourceAdapter


@dataclass
class TecOccurrenceReportDocumentRow:
    """Tasks 12502-12508: Document fields for TEC SITE_VISITS"""

    occurrence_report_id: int | None = None
    document_category_id: int | None = None
    document_sub_category_id: int | None = None
    description: str | None = None
    can_submitter_access: bool = False  # Task 12507: Default False
    name: str = ""  # Required by Document base model (blank=True but not null=True)


# Task 12502: document_category = "ORF Document"
DOCUMENT_CATEGORY_TRANSFORM = fk_lookup_static(
    model=DocumentCategory,
    lookup_field="document_category_name",
    static_value="ORF Document",
)

# Task 12503: document_sub_category = "Photo"
DOCUMENT_SUB_CATEGORY_TRANSFORM = fk_lookup_static(
    model=DocumentSubCategory,
    lookup_field="document_sub_category_name",
    static_value="Photo",
)


def occurrence_report_lookup_transform(value, ctx):
    """Task 12504: Map SITE_VISIT_ID to OccurrenceReport via migrated_from_id"""
    if not hasattr(occurrence_report_lookup_transform, "_cache"):
        mapping = dict(
            OccurrenceReport.objects.filter(migrated_from_id__isnull=False).values_list("migrated_from_id", "pk")
        )
        occurrence_report_lookup_transform._cache = mapping

    if value in (None, ""):
        return _result(None)

    val_str = str(value)
    unique_id = occurrence_report_lookup_transform._cache.get(val_str)

    if unique_id:
        return _result(unique_id)

    from boranga.components.data_migration.registry import TransformIssue

    return _result(
        value,
        TransformIssue("error", f"OccurrenceReport with migrated_from_id='{value}' not found"),
    )


# PIPELINES for TEC documents
PIPELINES = {
    "occurrence_report_id": ["strip", "required", occurrence_report_lookup_transform],
    "document_category_id": [DOCUMENT_CATEGORY_TRANSFORM],
    "document_sub_category_id": [DOCUMENT_SUB_CATEGORY_TRANSFORM],
    "description": ["strip", "blank_to_none"],  # Task 12508: SV_PHOTO as description
    "can_submitter_access": [static_value_factory(False)],  # Task 12507
    "name": [static_value_factory("")],  # Required by Document base model
}


class OccurrenceReportDocumentTecAdapter(SourceAdapter):
    """
    Tasks 12502-12508: Adapter for TEC SITE_VISITS documents (SV_PHOTO field)
    """

    source_key = Source.TEC_SITE_VISITS.value
    domain = "occurrence_report_document"
    PIPELINES = PIPELINES

    def extract(self, path: str, **options) -> ExtractionResult:
        """
        Extract document records from SITE_VISITS.csv where SV_PHOTO is present
        """
        raw_rows, warnings = self.read_table(path)

        rows = []
        for raw in raw_rows:
            # Task 12508: Only create document record if SV_PHOTO has value
            sv_photo = raw.get("SV_PHOTO", "").strip()
            if not sv_photo:
                continue

            site_visit_id = raw.get("SITE_VISIT_ID", "").strip()
            if not site_visit_id:
                continue

            # Create canonical row
            canonical = {
                # Task 12504: Link via SITE_VISIT_ID (matches migrated_from_id pattern)
                "occurrence_report_id": f"tec-site-{site_visit_id}",
                # Task 12508: SV_PHOTO becomes description
                "description": sv_photo,
            }

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)
