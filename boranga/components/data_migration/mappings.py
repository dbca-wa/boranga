from __future__ import annotations

import csv
import logging
from typing import Any, Literal

from django.conf import settings
from django.contrib.contenttypes.models import ContentType

from boranga.components.data_migration.registry import (
    TransformContext,
    TransformIssue,
    TransformResult,
    _result,
    registry,
)
from boranga.components.main.models import LegacyValueMap

logger = logging.getLogger(__name__)


# Cache: (legacy_system, list_name) -> dict[norm_legacy_value] = mapping dict
_CACHE: dict[tuple[str, str], dict[str, dict]] = {}


def _norm(s: Any) -> str:
    return str(s).strip().casefold()


def preload_map(legacy_system: str, list_name: str, active_only: bool = True):
    key = (legacy_system, list_name)
    if key in _CACHE:
        return
    qs = LegacyValueMap.objects.filter(
        legacy_system=legacy_system,
        list_name=list_name,
    )
    if active_only:
        qs = qs.filter(active=True)
    data: dict[str, dict] = {}
    for row in qs.select_related("target_content_type"):
        data[_norm(row.legacy_value)] = {
            "target_id": row.target_object_id,
            "content_type_id": row.target_content_type_id,
            "canonical": row.canonical_name,
            "raw": row.legacy_value,
        }
    _CACHE[key] = data


ReturnMode = Literal["id", "canonical", "both"]


def build_legacy_map_transform(
    legacy_system: str,
    list_name: str,
    *,
    required: bool = True,
    return_type: ReturnMode = "id",
):
    """
    Create (or return existing) transform that maps legacy enumerated values.
    return_type:
        id         -> returns target_object_id
        canonical  -> returns canonical_name (fallback to raw legacy value)
        both       -> returns tuple (target_id, canonical_name)
    """
    transform_name = (
        f"legacy_map_{legacy_system.lower()}_{list_name.lower()}_{return_type}"
    )
    if transform_name in registry._fns:
        return transform_name

    def fn(value, ctx: TransformContext):
        if value in (None, ""):
            if required:
                return TransformResult(
                    value=None,
                    issues=[TransformIssue("error", f"{list_name} required")],
                )
            return _result(None)
        preload_map(legacy_system, list_name)
        table = _CACHE[(legacy_system, list_name)]
        norm = _norm(value)
        if norm not in table:
            return TransformResult(
                value=None,
                issues=[
                    TransformIssue(
                        "error", f"Unmapped {legacy_system}.{list_name} value '{value}'"
                    )
                ],
            )
        entry = table[norm]
        canonical = entry["canonical"] or entry["raw"]
        if return_type == "id":
            return _result(entry["target_id"])
        if return_type == "canonical":
            return _result(canonical)
        if return_type == "both":
            return _result((entry["target_id"], canonical))
        # Fallback (should not happen with Literal typing)
        return _result(entry["target_id"])

    registry._fns[transform_name] = fn
    return transform_name


def load_species_to_region_links(
    legacy_system: str,
    path: str | None = None,
    key_column: str = "TXN_LST_ID",
    region_column: str = "region_key",
    delimiter: str = ";",
) -> dict[str, list[str]]:
    """
    Build a mapping: migrated_from_id -> list of legacy region keys.

    This version assumes a CSV source (path or settings.LEGACY_SPECIES_REGION_PATH)
    and does not attempt any model-based fallback.
    """
    mapping: dict[str, list[str]] = {}

    csv_path = path or getattr(settings, "LEGACY_SPECIES_REGION_PATH", None)
    if not csv_path:
        logger.warning(
            "No CSV path provided for species->region links (legacy_system=%s)",
            legacy_system,
        )
        return mapping

    try:
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if (
                not reader.fieldnames
                or key_column not in reader.fieldnames
                or region_column not in reader.fieldnames
            ):
                logger.warning(
                    "CSV %s missing expected columns (%s, %s); found: %s",
                    csv_path,
                    key_column,
                    region_column,
                    reader.fieldnames,
                )
                return mapping
            for row in reader:
                key = (row.get(key_column) or "").strip()
                if not key:
                    continue
                raw = (row.get(region_column) or "").strip()
                if not raw:
                    continue
                if delimiter and delimiter in raw:
                    parts = [p.strip() for p in raw.split(delimiter) if p.strip()]
                else:
                    parts = [raw] if raw else []
                if parts:
                    mapping.setdefault(key, []).extend(parts)
        return mapping
    except FileNotFoundError:
        logger.warning("Species->region CSV path not found: %s", csv_path)
        return mapping
    except Exception as exc:
        logger.exception("Error reading species->region CSV %s: %s", csv_path, exc)
        return mapping

    # ...existing code...


def load_species_to_district_links(
    legacy_system: str,
    path: str | None = None,
    key_column: str = "TXN_LST_ID",
    district_column: str = "district_key",
    delimiter: str = ";",
) -> dict[str, list[str]]:
    """
    Build a mapping: migrated_from_id -> list of legacy district keys.

    This version assumes a CSV source (path or settings.LEGACY_SPECIES_DISTRICT_PATH)
    and does not attempt any model-based fallback.
    """
    mapping: dict[str, list[str]] = {}

    csv_path = path or getattr(settings, "LEGACY_SPECIES_DISTRICT_PATH", None)
    if not csv_path:
        logger.warning(
            "No CSV path provided for species->district links (legacy_system=%s)",
            legacy_system,
        )
        return mapping

    try:
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if (
                not reader.fieldnames
                or key_column not in reader.fieldnames
                or district_column not in reader.fieldnames
            ):
                logger.warning(
                    "CSV %s missing expected columns (%s, %s); found: %s",
                    csv_path,
                    key_column,
                    district_column,
                    reader.fieldnames,
                )
                return mapping
            for row in reader:
                key = (row.get(key_column) or "").strip()
                if not key:
                    continue
                raw = (row.get(district_column) or "").strip()
                if not raw:
                    continue
                if delimiter and delimiter in raw:
                    parts = [p.strip() for p in raw.split(delimiter) if p.strip()]
                else:
                    parts = [raw] if raw else []
                if parts:
                    mapping.setdefault(key, []).extend(parts)
        return mapping
    except FileNotFoundError:
        logger.warning("Species->district CSV path not found: %s", csv_path)
        return mapping
    except Exception as exc:
        logger.exception("Error reading species->district CSV %s: %s", csv_path, exc)
        return mapping


def load_legacy_to_pk_map(
    legacy_system: str,
    model_name: str,
    app_label: str = "boranga",
) -> dict[str, int]:
    """
    Return a mapping of legacy_value -> target model PK for entries registered
    in LegacyValueMap for the given legacy_system and model.

    Strategy:
    - Prefer LegacyValueMap rows whose target_content_type matches the requested model.
    - Fallback to LegacyValueMap rows where list_name == model_name (if any).
    - Only active=True entries are considered.
    """
    mapping: dict[str, int] = {}

    # resolve content type for the target model if possible
    try:
        ct = ContentType.objects.get(app_label=app_label, model=model_name.lower())
    except ContentType.DoesNotExist:
        ct = None

    qs = LegacyValueMap.objects.filter(legacy_system__iexact=legacy_system, active=True)
    if ct is not None:
        qs = qs.filter(target_content_type=ct)
    else:
        qs = qs.filter(list_name__iexact=model_name)

    for row in qs.select_related("target_content_type"):
        if row.target_object_id:
            key = str(row.legacy_value).strip()
            if key:
                # later entries may override earlier; keep last-seen
                mapping[key] = int(row.target_object_id)

    logger.debug(
        "load_legacy_to_pk_map: loaded %d mappings for %s -> %s",
        len(mapping),
        legacy_system,
        model_name,
    )
    return mapping
