from boranga.components.data_migration.registry import (
    emailuser_by_legacy_username_factory,
    fk_lookup,
)
from boranga.components.main.models import LegacyValueMap
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
    "description": ["strip", "blank_to_none"],
    "document_category_id": ["identity"],
    "document_sub_category_id": ["identity"],
}


class OccurrenceDocumentTpflAdapter(SourceAdapter):
    source_key = Source.TPFL.value
    domain = "occurrence_report_documents"
    DEFAULT_CATEGORY_NAME = "Correspondence"
    DEFAULT_SUBCATEGORY_NAME = "TPFL Liaison Record"

    def __init__(self):
        self._doc_category_id: int | None = None
        self._doc_sub_category_id: int | None = None

    def _resolve_default_categories(self) -> tuple[int, int]:
        """Cache lookups for default category/sub-category IDs."""

        if self._doc_category_id is None:
            try:
                doc_cat = DocumentCategory.objects.get(
                    document_category_name=self.DEFAULT_CATEGORY_NAME
                )
            except DocumentCategory.DoesNotExist:
                raise ValueError(
                    f"DocumentCategory '{self.DEFAULT_CATEGORY_NAME}' does not exist"
                )
            self._doc_category_id = doc_cat.id

        if self._doc_sub_category_id is None:
            try:
                doc_sub_cat = DocumentSubCategory.objects.get(
                    document_sub_category_name=self.DEFAULT_SUBCATEGORY_NAME
                )
            except DocumentSubCategory.DoesNotExist:
                raise ValueError(
                    "DocumentSubCategory 'TPFL Liaison Record' does not exist"
                )
            self._doc_sub_category_id = doc_sub_cat.id

        return self._doc_category_id, self._doc_sub_category_id

    def _map_notification_action(self, legacy_value: str | None) -> str | None:
        if not legacy_value:
            return None

        canonical = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="NOTIFICATION_TYPE",
            legacy_value=str(legacy_value).strip(),
        )

        if not canonical:
            return None

        canonical_str = str(canonical).strip()
        if not canonical_str or canonical_str.upper() == "__IGNORE__":
            return None
        return canonical_str

    def _build_description(self, raw: dict) -> str | None:
        parts: list[str] = []

        othernames = str(raw.get("OTHERNAMES", "")).strip()
        if othernames:
            parts.append(f"Name: {othernames}")

        notification_action = self._map_notification_action(
            raw.get("NOTIFICATION_TYPE")
        )
        if notification_action:
            parts.append(f"Action: {notification_action}")

        notifier_name = str(raw.get("NOTIFIER_NAME", "")).strip()
        if notifier_name:
            parts.append(f"Liaising Officer: {notifier_name}")

        comments = str(raw.get("COMMENTS", "")).strip()
        if comments:
            parts.append(comments)

        if not parts:
            return None
        return "; ".join(parts)

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        doc_cat_id, doc_sub_cat_id = self._resolve_default_categories()

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)

            # Prepend source prefix to occurrence_id
            mid = canonical.get("occurrence_id")
            if mid and not str(mid).startswith(f"{Source.TPFL.value.lower()}-"):
                canonical["occurrence_id"] = f"{Source.TPFL.value.lower()}-{mid}"

            canonical["document_category_id"] = doc_cat_id
            canonical["document_sub_category_id"] = doc_sub_cat_id
            description = self._build_description(raw)
            if description:
                canonical["description"] = description
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)


# Attach pipelines to adapter
OccurrenceDocumentTpflAdapter.PIPELINES = PIPELINES
