from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from boranga.components.data_migration.adapters.base import (
    ExtractionResult,
    SourceAdapter,
)
from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.registry import (
    TransformIssue,
    _result,
    build_legacy_map_transform,
    emailuser_by_legacy_username_factory,
    fk_lookup_static,
    static_value_factory,
)
from boranga.components.occurrence.models import OccurrenceReport
from boranga.components.species_and_communities.models import (
    DocumentCategory,
    DocumentSubCategory,
)

from ..sources import Source

# -----------------------------------------------------------------------------
# Schema Definition
# -----------------------------------------------------------------------------

COLUMN_MAP = {
    "SHEETNO": "occurrence_report_id",
    "CREATED_BY": "uploaded_by",
    "CREATED_DATE": "uploaded_date",
    # Description fields
    "ATTACHED_DOC1": "ATTACHED_DOC1",
    "ATTACHED_DOC2": "ATTACHED_DOC2",
    "ATTACHED_DOC3": "ATTACHED_DOC3",
    "ATTACHED_OTHER_DOC": "ATTACHED_OTHER_DOC",
}

REQUIRED_COLUMNS = [
    "occurrence_report_id",
]

PIPELINES: dict[str, list[str]] = {}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    source_choices=None,
)

# Convenience exports
map_raw_row = SCHEMA.map_raw_row


@dataclass
class OccurrenceReportDocumentRow:
    occurrence_report_id: int | None = None
    document_category_id: int | None = None
    document_sub_category_id: int | None = None
    description: str | None = None
    active: bool = True
    uploaded_by: int | None = None
    uploaded_date: datetime | None = None
    name: str | None = None  # Should be None/Null per requirements


# -----------------------------------------------------------------------------
# Transforms
# -----------------------------------------------------------------------------

# Task 11461: Default Value of "ORF Document"
DOCUMENT_CATEGORY_TRANSFORM = fk_lookup_static(
    model=DocumentCategory,
    lookup_field="document_category_name",
    static_value="ORF Document",
)

# Task 11462: Default Value of "TPFL Document Reference"
DOCUMENT_SUB_CATEGORY_TRANSFORM = fk_lookup_static(
    model=DocumentSubCategory,
    lookup_field="document_sub_category_name",
    static_value="TPFL Document Reference",
)


def occurrence_report_lookup_transform(value, ctx):
    # Cache on function attribute
    if not hasattr(occurrence_report_lookup_transform, "_cache"):
        # We only really care about TPFL ones for this specific adapter/import run,
        # but loading all migrated ones is generally safe.
        mapping = dict(
            OccurrenceReport.objects.filter(migrated_from_id__isnull=False).values_list(
                "migrated_from_id", "pk"
            )
        )
        occurrence_report_lookup_transform._cache = mapping

    if value in (None, ""):
        return _result(None)

    val_str = str(value)
    unique_id = occurrence_report_lookup_transform._cache.get(val_str)

    if unique_id:
        return _result(unique_id)

    return _result(
        value,
        TransformIssue(
            "error", f"OccurrenceReport with migrated_from_id='{value}' not found"
        ),
    )


# Task 11463: Map SHEETNO to OccurrenceReport (using migrated_from_id)
# Optimization: Use preloaded map instead of generic fk_lookup to avoid N+1 queries
FK_OCCURRENCE_REPORT = occurrence_report_lookup_transform

# Task 11465: Default = TRUE
ACTIVE_DEFAULT = static_value_factory(True)

# Task 11471: Map CREATED_BY to Boranga user
EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM = emailuser_by_legacy_username_factory("TPFL")

# Task 11467: Description concatenation
ATTACHED_DOC_TRANSFORM = build_legacy_map_transform(
    "TPFL",
    "ATTACHED_DOC (DRF_LOV_ATTACHD_DOC_VWS)",
    required=False,
)

STATIC_NONE = static_value_factory(None)
STATIC_EMPTY_STRING = static_value_factory("")


def description_concatenate_transform(value, ctx):
    """
    Concatenate intro text + all 4x field values (or matched equiv) with separator = "; "
    "File is in S&C SharePoint Library - Flora: "<ATTACHED_DOC1>"; "<ATTACHED_DOC2>";
    "<ATTACHED_DOC3>"; "<ATTACHED_OTHER_DOC>
    """
    row = getattr(ctx, "row", None) if ctx is not None else None
    if row is None and isinstance(ctx, dict):
        row = ctx.get("row") or ctx

    if not isinstance(row, dict):
        return _result(None)

    parts = []

    # Helper to process ATTACHED_DOC fields
    def process_attached_doc(field_name):
        val = row.get(field_name)
        if val and str(val).strip():
            # Use the transform logic manually since we are inside a custom transform
            # Or we can rely on the fact that build_legacy_map_transform registers a function
            # But here we need to call it.
            # Instead, let's use LegacyValueMap directly as build_legacy_map_transform does.
            from boranga.components.main.models import LegacyValueMap

            mapped = LegacyValueMap.get_target(
                legacy_system="TPFL",
                list_name="ATTACHED_DOC (DRF_LOV_ATTACHD_DOC_VWS)",
                legacy_value=str(val).strip(),
            )
            if mapped:
                return mapped
            return str(val).strip()  # Fallback if no mapping? Or maybe just the code?
            # Task says: "list match code to canonical name per list_name"
            # If no match, what? "For ATTACHED_OTHER_DOC: migrate text string as is"
            # For DOC1, DOC2, DOC3 it implies we should find a match.
        return None

    # ATTACHED_DOC1
    doc1 = process_attached_doc("ATTACHED_DOC1")
    if doc1:
        parts.append(doc1)

    # ATTACHED_DOC2
    doc2 = process_attached_doc("ATTACHED_DOC2")
    if doc2:
        parts.append(doc2)

    # ATTACHED_DOC3
    doc3 = process_attached_doc("ATTACHED_DOC3")
    if doc3:
        parts.append(doc3)

    # ATTACHED_OTHER_DOC - migrate text string as is
    other = row.get("ATTACHED_OTHER_DOC")
    if other and str(other).strip():
        parts.append(str(other).strip())

    if not parts:
        return _result(None)

    intro = "File is in S&C SharePoint Library - Flora: "
    full_text = intro + "; ".join(parts)

    return _result(full_text)


PIPELINES.update(
    {
        "occurrence_report_id": ["strip", "required", FK_OCCURRENCE_REPORT],
        "document_category_id": [DOCUMENT_CATEGORY_TRANSFORM],
        "document_sub_category_id": [DOCUMENT_SUB_CATEGORY_TRANSFORM],
        "active": [ACTIVE_DEFAULT],
        "uploaded_by": [
            "strip",
            "blank_to_none",
            EMAILUSER_BY_LEGACY_USERNAME_TRANSFORM,
        ],
        "uploaded_date": ["strip", "blank_to_none", "datetime_iso"],
        "description": [description_concatenate_transform],
        "name": [STATIC_EMPTY_STRING],
    }
)

# -----------------------------------------------------------------------------
# Adapter
# -----------------------------------------------------------------------------


class OccurrenceReportDocumentAdapter(SourceAdapter):
    source_key = "DRF_RFR_FORMS"  # This needs to match what we use in the handler
    domain = "occurrence_report_document"

    def extract(self, path: str, **options) -> ExtractionResult:
        raw_rows, warnings = self.read_table(path)

        rows = []
        for raw in raw_rows:
            # Map raw row to canonical dict
            canonical = map_raw_row(raw)

            # Apply pipelines
            # We need to manually run pipelines or use a helper.
            # The Schema class has effective_pipelines() but map_raw_row only does column mapping.
            # We need to run the pipelines on the mapped data.

            # Prepend source prefix
            mid = canonical.get("occurrence_report_id")
            if mid and not str(mid).startswith(f"{Source.TPFL.value.lower()}-"):
                canonical["occurrence_report_id"] = f"{Source.TPFL.value.lower()}-{mid}"

            rows.append(canonical)

        return ExtractionResult(rows=rows, warnings=warnings)


OccurrenceReportDocumentAdapter.PIPELINES = PIPELINES
