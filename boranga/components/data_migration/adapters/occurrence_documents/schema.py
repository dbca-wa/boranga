from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from boranga.components.data_migration.adapters.schema_base import Schema

COLUMN_MAP = {
    "POP_ID": "occurrence_id",
    "CREATED_BY": "uploaded_by",
    "NOTIFY_DATE": "uploaded_date",
}

REQUIRED_COLUMNS = [
    "occurrence_id",
    "uploaded_by",
    "uploaded_date",
]

PIPELINES: dict[str, list[str]] = {}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    source_choices=None,
)

# Convenience exports
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class OccurrenceDocumentRow:
    """
    Canonical (post-transform) occurrence document data for persistence.
    """

    occurrence_id: str
    uploaded_by: int
    uploaded_date: datetime
    document_category_id: int | None = None
    document_subcategory_id: int | None = None
    description: str | None = None
