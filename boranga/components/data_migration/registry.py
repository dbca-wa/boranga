from __future__ import annotations

import csv
import hashlib
import logging
import os
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from datetime import timezone as stdlib_timezone
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Literal

import shapely.wkt
from django.contrib.gis.geos import GEOSGeometry
from django.core.cache import cache
from django.db import models
from django.utils import timezone
from ledger_api_client.ledger_models import EmailUserRO
from pyproj import Transformer
from shapely.geometry import Point
from shapely.ops import transform as shapely_transform

from boranga.components.data_migration import mappings as dm_mappings
from boranga.components.main.models import (
    LegacyUsernameEmailuserMapping,
    LegacyValueMap,
    MigrationRun,
    neutralise_html,
)
from boranga.components.species_and_communities.models import (
    GroupType,
    Taxonomy,
    TaxonPreviousName,
)

TransformFn = Callable[[Any, "TransformContext"], "TransformResult"]


logger = logging.getLogger(__name__)


@dataclass
class TransformIssue:
    level: str  # 'error' | 'warning' | 'info'
    message: str


@dataclass
class TransformResult:
    value: Any
    issues: list[TransformIssue] = field(default_factory=list)

    @property
    def errors(self):
        return [i for i in self.issues if i.level == "error"]

    @property
    def warnings(self):
        return [i for i in self.issues if i.level == "warning"]


@dataclass
class TransformContext:
    row: dict[str, Any]
    model: type[models.Model] | None = None
    user_id: int | None = None


class TransformRegistry:
    def __init__(self):
        self._fns: dict[str, TransformFn] = {}

    def register(self, name: str):
        def deco(fn: TransformFn):
            self._fns[name] = fn
            return fn

        return deco

    def build_pipeline(self, names):
        pipeline = []
        for n in names:
            if callable(n):
                pipeline.append(n)
                continue
            try:
                pipeline.append(self._fns[n])
            except KeyError:
                raise KeyError(f"unknown transform {n!r} (pipeline entry must be a registered name or a callable)")
        return pipeline


registry = TransformRegistry()


def _result(value, *issues: TransformIssue):
    return TransformResult(value=value, issues=list(issues))


@registry.register("static_value_True")
def t_static_value_True(value, ctx):
    return _result(True)


@registry.register("static_value_boranga.tec@dbca.wa.gov.au")
def t_static_value_boranga_tec(value, ctx):
    return _result("boranga.tec@dbca.wa.gov.au")


@registry.register("static_value_community")
def t_static_value_community(value, ctx):
    return _result("community")


@registry.register("Y_to_active_else_historical")
def t_Y_to_active_else_historical(value, ctx):
    if value == "Y":
        return _result("active")
    return _result("historical")


@registry.register("y_to_true_n_to_none")
def t_y_to_true_n_to_none(value, ctx):
    if not value:
        return _result(None)
    val = str(value).strip().upper()
    if val == "Y":
        return _result(True)
    if val == "N":
        return _result(None)
    return _result(None)


@registry.register("y_to_true_else_false")
def t_y_to_true_else_false(value, ctx):
    if not value:
        return _result(False)
    val = str(value).strip().upper()
    return _result(val == "Y")


@registry.register("is_present")
def t_is_present(value, ctx):
    return _result(bool(value))


@registry.register("strip")
def t_strip(value, ctx):
    if value is None:
        return _result(None)
    return _result(str(value).strip())


@registry.register("normalise_whitespace")
def t_normalise_whitespace(value, ctx):
    if value is None:
        return _result(None)
    return _result(re.sub(r"\s+", " ", str(value)).strip())


@registry.register("blank_to_none")
def t_blank_to_none(value, ctx):
    if isinstance(value, str) and value.strip() == "":
        return _result(None)
    return _result(value)


@registry.register("null_to_none")
def t_null_to_none(value, ctx):
    if value in ("Null", "NULL", "null", "None", "none"):
        return _result(None)
    return _result(value)


@registry.register("identity")
def t_identity(value, ctx):
    return _result(value)


@registry.register("required")
def t_required(value, ctx):
    if value in (None, "", []):
        return _result(value, TransformIssue("error", "Value required"))
    return _result(value)


def choices_transform(choices: Iterable[str]):
    norm = [c.lower() for c in choices]
    choice_set = {c.lower(): c for c in choices}
    h = hashlib.sha1((":".join(sorted(norm))).encode()).hexdigest()[:8]
    name = f"choices_{h}"
    if name in registry._fns:
        return name

    def inner(value, ctx):
        if value is None:
            return _result(None)
        key = str(value).lower().strip()
        if key not in choice_set:
            return _result(value, TransformIssue("error", f"Invalid choice '{value}'"))
        return _result(choice_set[key])

    registry._fns[name] = inner
    return name


@registry.register("to_int")
def t_to_int(value, ctx):
    if value in (None, ""):
        return _result(None)
    try:
        return _result(int(value))
    except (ValueError, TypeError):
        return _result(value, TransformIssue("error", f"Not an integer: {value!r}"))


@registry.register("to_decimal")
def t_to_decimal(value, ctx):
    from decimal import Decimal, InvalidOperation

    if value in (None, ""):
        return _result(None)
    try:
        return _result(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return _result(value, TransformIssue("error", f"Not a decimal: {value!r}"))


def to_decimal_factory(max_digits: int | None = None, decimal_places: int | None = None):
    """
    Return a registered transform name that converts value -> Decimal and optionally
    enforces max_digits (total digits) and decimal_places (scale).

    Usage:
      D10_2 = to_decimal_factory(max_digits=10, decimal_places=2)
      PIPELINES["amount"] = ["strip", "blank_to_none", D10_2]
    """
    key = f"to_decimal_max{max_digits}_dp{decimal_places}"
    name = "to_decimal_" + hashlib.sha1(key.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(value, ctx):
        if value in (None, ""):
            return _result(None)
        try:
            dec = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return _result(value, TransformIssue("error", f"Not a decimal: {value!r}"))

        # enforce decimal places by quantizing
        if decimal_places is not None:
            try:
                quant = Decimal(f"1e-{int(decimal_places)}")
                dec = dec.quantize(quant, rounding=ROUND_HALF_EVEN)
            except Exception as e:
                return _result(
                    value,
                    TransformIssue("error", f"Failed to quantize to {decimal_places} dp: {e}"),
                )

        # enforce max digits (total significant digits excluding sign)
        if max_digits is not None:
            tup = dec.as_tuple()
            total_digits = len(tup.digits)
            if total_digits > int(max_digits):
                return _result(
                    value,
                    TransformIssue(
                        "error",
                        f"Decimal {dec!r} exceeds max_digits={max_digits} (has {total_digits} digits)",
                    ),
                )

        return _result(dec)

    registry._fns[name] = _inner
    return name


@registry.register("date_iso")
def t_date_iso(value, ctx):
    if not value:
        return _result(None)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return _result(datetime.strptime(str(value), fmt).date())
        except ValueError:
            pass
    return _result(value, TransformIssue("error", f"Unrecognized date format: {value!r}"))


def _parse_datetime_iso(value: Any, default_tz: Any = None) -> TransformResult:
    """
    Parse datetimes (including ISO strings) and return a timezone-aware datetime
    in UTC suitable for Django (USE_TZ=True).

    If the incoming string contains an explicit UTC offset token (+0000 / +00:00 / Z)
    we strip that token and treat the remainder as naive. This remainder will be
    made aware using `default_tz` (if provided) or UTC.

    This behaviour is useful when source systems emit +0000 style offsets for
    local times (mojibake-like timezone handling) or when some parsers don't
    accept +0000.
    """
    if not value:
        return _result(None)

    s = str(value).strip()
    dt = None

    # If the string ends with a timezone offset, capture it.
    m = re.search(r"([+-]\d{2}:?\d{2}|Z)$", s)
    if m:
        tz_token = m.group(1)
        # Treat explicit UTC tokens as no-offset (strip them and parse as naive,
        # then we'll apply default_tz or UTC later)
        if tz_token in ("Z", "+0000", "+00:00"):
            # remove trailing offset (handles both +0000 and +00:00)
            s = s[: m.start(1)].rstrip()
        # else: leave non-UTC offsets intact so parser can handle/convert them

    # try stdlib ISO parsing first (handles offsets like +00:00; note +0000 not accepted)
    try:
        # if string still uses trailing 'Z' (unlikely now) normalize for fromisoformat
        iso = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(iso)
    except Exception:
        dt = None

    # try python-dateutil if installed (very permissive)
    if dt is None:
        try:
            from dateutil import parser

            dt = parser.parse(s)
        except Exception:
            dt = None

    # fallback to explicit legacy formats
    if dt is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                dt = None

    if dt is None:
        return _result(value, TransformIssue("error", f"Unrecognized datetime format: {value!r}"))

    # Make timezone-aware
    if dt.tzinfo is None:
        # Resolve default_tz to a timezone object
        tz_obj = default_tz
        if isinstance(tz_obj, str):
            try:
                from zoneinfo import ZoneInfo

                tz_obj = ZoneInfo(tz_obj)
            except ImportError:
                # fall back to pytz if on older python/django or if preferred
                import pytz

                tz_obj = pytz.timezone(tz_obj)

        if tz_obj is None:
            tz_obj = stdlib_timezone.utc

        # treat naive datetimes as being in tz_obj and make them aware
        dt = timezone.make_aware(dt, tz_obj)

    # convert any aware datetime (including those just made aware) to UTC for storage
    dt = dt.astimezone(stdlib_timezone.utc)

    return _result(dt)


@registry.register("datetime_iso")
def t_datetime_iso(value, ctx):
    """
    Parse datetimes (including ISO strings) and return a timezone-aware datetime
    in UTC suitable for Django (USE_TZ=True).

    If the incoming string contains an explicit UTC offset token (+0000 / +00:00 / Z)
    we strip that token and treat the remainder as naive UTC (useful when source
    emits +0000 style offsets that some parsers don't accept).
    """
    return _parse_datetime_iso(value)


def datetime_iso_factory(default_tz: str | None = None) -> str:
    """
    Return a registered transform name that parses datetimes (ISO or legacy)
    and returns a UTC-aware datetime.

    If the source has +0000/+00:00/Z but is actually local time, or if the
    source is naive, `default_tz` can be used to correctly localise before
    conversion to UTC.

    Parameters:
      - default_tz: optional timezone name (e.g. 'Australia/Perth') or tzinfo

    Usage:
      PERTH_DT = datetime_iso_factory('Australia/Perth')
      PIPELINES['created_at'] = [PERTH_DT]
    """
    key = f"datetime_iso_{default_tz or 'UTC'}"
    name = "datetime_iso_" + hashlib.sha1(key.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    @registry.register(name)
    def inner(value, ctx):
        return _parse_datetime_iso(value, default_tz=default_tz)

    return name


@registry.register("date_from_datetime_iso")
def t_date_from_datetime_iso(value, ctx):
    """
    Parse a datetime-like string (ISO, with offsets, or legacy formats) and
    return a datetime.date suitable for Django DateField.

    Behaviour:
    - strips explicit UTC tokens (+0000 / +00:00 / Z) and treats remainder as UTC,
    - parses aware datetimes and converts to UTC, then returns the UTC date,
    - parses naive datetimes as UTC and returns the date.
    - on parse failure returns the original value with a TransformIssue.
    """
    res = _parse_datetime_iso(value)
    if res.errors:
        return res
    if res.value:
        res.value = res.value.date()
    return res


def date_from_datetime_iso_factory(default_tz: str | None = None) -> str:
    """
    Return a registered transform name that parses datetimes (ISO or legacy)
    and returns a UTC date.

    See datetime_iso_factory for details on default_tz.

    Usage:
      PERTH_DATE = date_from_datetime_iso_factory('Australia/Perth')
      PIPELINES['observed_at'] = [PERTH_DATE]
    """
    key = f"date_from_datetime_iso_{default_tz or 'UTC'}"
    name = "date_from_datetime_iso_" + hashlib.sha1(key.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    @registry.register(name)
    def inner(value, ctx):
        res = _parse_datetime_iso(value, default_tz=default_tz)
        if res.errors:
            return res
        if res.value:
            res.value = res.value.date()
        return res

    return name


@registry.register("ocr_comments_transform")
def t_ocr_comments_transform(value, ctx):
    PURPOSE1 = (ctx.row.get("PURPOSE1") or "").strip()
    PURPOSE2 = (ctx.row.get("PURPOSE2") or "").strip()
    VESTING = (ctx.row.get("VESTING") or "").strip()
    FENCING_STATUS = (ctx.row.get("FENCING_STATUS") or "").strip()
    FENCING_COMMENTS = (ctx.row.get("FENCING_COMMENTS") or "").strip()
    ROADSIDE_MARKER_STATUS = (ctx.row.get("ROADSIDE_MARKER_STATUS") or "").strip()
    RDSIDE_MKR_COMMENTS = (ctx.row.get("RDSIDE_MKR_COMMENTS") or "").strip()

    comments = None
    transform_issues: list[TransformIssue] = []

    # Helper to append parts safely (skips falsy parts and avoids None concatenation)
    def _append_part(part: str | None):
        nonlocal comments
        if not part:
            return
        if comments:
            comments += ", " + part
        else:
            comments = part

    if PURPOSE1:
        purpose1 = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="PURPOSE (DRF_LOV_PURPOSE_VWS)",
            legacy_value=PURPOSE1,
            use_cache=True,
        )
        if not purpose1:
            transform_issues.append(
                TransformIssue("error", f"No Purpose found that maps to legacy value: {PURPOSE1!r}")
            )
        else:
            _append_part(purpose1)

    if PURPOSE2:
        purpose2 = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="PURPOSE (DRF_LOV_PURPOSE_VWS)",
            legacy_value=PURPOSE2,
            use_cache=True,
        )
        if not purpose2:
            transform_issues.append(
                TransformIssue("error", f"No Purpose found that maps to legacy value: {PURPOSE2!r}")
            )
        else:
            _append_part(purpose2)

    if VESTING:
        vesting = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="VESTING (DRF_LOV_VESTING_VWS)",
            legacy_value=VESTING,
            use_cache=True,
        )
        if not vesting:
            transform_issues.append(TransformIssue("error", f"No Vesting found that maps to legacy value: {VESTING!r}"))
        else:
            _append_part(f"Vesting: {vesting}")

    _append_part(f"Fencing Status: {FENCING_STATUS}" if FENCING_STATUS else None)
    _append_part(f"Fencing Comments: {FENCING_COMMENTS}" if FENCING_COMMENTS else None)
    _append_part(f"Roadside Marker Status: {ROADSIDE_MARKER_STATUS}" if ROADSIDE_MARKER_STATUS else None)
    _append_part(f"Roadside Marker Comments: {RDSIDE_MKR_COMMENTS}" if RDSIDE_MKR_COMMENTS else None)

    return _result(comments, *transform_issues)


def fk_lookup(model: type[models.Model], lookup_field: str = "id"):
    key = f"fk_{model._meta.label_lower}_{lookup_field}"

    # Cache lookup results to avoid repeated DB queries for the same input value
    # Key: input value, Value: (result_value, list[TransformIssue])
    _lookup_cache = {}

    @registry.register(key)
    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)

        if value in _lookup_cache:
            res_val, res_issues = _lookup_cache[value]
            return _result(res_val, *res_issues)

        qs = model._default_manager

        # Try lookup using the stored (cleaned) form first, then the raw value.
        try:
            cleaned = neutralise_html(str(value))
        except Exception:
            cleaned = None

        candidates = []
        # If this row originates from TPFL, try a fast CSV-backed lookup (NAME -> nomos canonical)
        try:
            src = ctx.row.get("_source") if isinstance(ctx.row, dict) else None
        except Exception:
            src = None
        if src == "TPFL":
            try:
                tpfl_map = _load_tpfl_name_to_nomos()
                if tpfl_map:
                    # the TPFL adapter sets NAME on the source row; normalise it the same way
                    tpfl_name = ctx.row.get("NAME")
                    if tpfl_name:
                        tpfl_name_key = str(tpfl_name).replace("\u00a0", " ").replace("\u202f", " ").strip()
                        mapped = tpfl_map.get(tpfl_name_key)
                        if mapped:
                            # prefer cleaned mapping first
                            try:
                                mapped_clean = neutralise_html(mapped)
                            except Exception:
                                mapped_clean = None
                            if mapped_clean:
                                candidates.append(mapped_clean)
                            candidates.append(mapped)
            except Exception:
                # be conservative: don't fail the transform if mapping load/lookup breaks
                logger.exception("TPFL mapping lookup failed")
        if cleaned and cleaned != str(value):
            candidates.append(cleaned)
        candidates.append(value)

        for candidate in candidates:
            try:
                obj = qs.get(**{lookup_field: candidate})
                res_val, res_issues = obj.pk, []
                _lookup_cache[value] = (res_val, res_issues)
                return _result(res_val, *res_issues)
            except model.DoesNotExist:
                continue
            except model.MultipleObjectsReturned:
                res_val = value
                res_issues = [
                    TransformIssue(
                        "error",
                        f"Multiple {model.__name__} with {lookup_field}='{value}' found",
                    )
                ]
                _lookup_cache[value] = (res_val, res_issues)
                return _result(res_val, *res_issues)

        res_val = value
        res_issues = [
            TransformIssue(
                "error",
                f"{model.__name__} with {lookup_field}='{value}' not found",
            )
        ]
        _lookup_cache[value] = (res_val, res_issues)
        return _result(res_val, *res_issues)

    return key


def fk_lookup_static(model: type[models.Model], lookup_field: str, static_value: Any) -> str:
    """
    Return a registered transform name that looks up a foreign key using a
    static/constant value (ignoring the incoming row value).

    This is useful when every row should map to the same FK object.

    The lookup result is cached for the duration of the data migration (per transform name),
    so the database query only happens once.

    Parameters:
      - model: the Django model to query
      - lookup_field: the field name to filter by (e.g. 'id', 'name', 'code')
      - static_value: the constant value to use for lookup

    Usage:
      FK_STATIC_GROUP = fk_lookup_static(GroupType, "name", "Flora")
      PIPELINES["group_type_id"] = [FK_STATIC_GROUP]

    Returns the primary key (id) of the matched object, or an error TransformIssue
    if the object is not found or multiple objects match.
    """
    key = (
        f"fk_static_{model._meta.label_lower}_{lookup_field}"
        + "_"
        + hashlib.sha1(str(static_value).encode()).hexdigest()[:8]
    )

    # Check if already registered (cached by value)
    if key in registry._fns:
        return key

    # Closure variable to cache the lookup result and any error
    cached_result = {"result": None, "error": None, "cached": False}

    @registry.register(key)
    def inner(value, ctx):
        # Return cached result if available
        if cached_result["cached"]:
            if cached_result["error"]:
                return _result(value, cached_result["error"])
            return _result(cached_result["result"])

        # Perform lookup on first call
        qs = model._default_manager

        try:
            obj = qs.get(**{lookup_field: static_value})
            cached_result["result"] = obj.pk
            cached_result["cached"] = True
            return _result(obj.pk)
        except model.DoesNotExist:
            error = TransformIssue(
                "error",
                f"{model.__name__} with {lookup_field}='{static_value}' not found",
            )
            cached_result["error"] = error
            cached_result["cached"] = True
            return _result(value, error)
        except model.MultipleObjectsReturned:
            error = TransformIssue(
                "error",
                f"Multiple {model.__name__} objects with {lookup_field}='{static_value}' found",
            )
            cached_result["error"] = error
            cached_result["cached"] = True
            return _result(value, error)

    return key


def lookup_model_value_factory(model_name: str, field_name: str, static_value: Any = None) -> str:
    """
    Return a registered transform name that looks up a model instance ID using a string reference.

    Parameters:
      - model_name: 'app_label.Model' name string (e.g. 'boranga.Species')
      - field_name: field to filter by (e.g. 'scientific_name')
      - static_value: if not None, use this value for lookup (ignoring row value).
                      If None, use the row value for lookup.
    """
    key_parts = ["lookup_model", model_name, field_name]
    if static_value is not None:
        key_parts.append(str(static_value))

    key_repr = ":".join(key_parts)
    name = "lookup_model_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]

    if name in registry._fns:
        return name

    # Cache lookup results to avoid repeated DB queries for the same input value
    # Key: input value, Value: (result_value, list[TransformIssue])
    _lookup_cache = {}

    @registry.register(name)
    def inner(value, ctx):
        from django.apps import apps
        from django.core.exceptions import FieldDoesNotExist

        # Determine value to search: either the static config or the row value
        search_value = static_value if static_value is not None else value

        # Return cached result if available
        if search_value in _lookup_cache:
            res_val, res_issues = _lookup_cache[search_value]
            return _result(res_val, *res_issues)

        # Resolve Model
        try:
            Model = apps.get_model("boranga", model_name)
        except LookupError:
            r = (None, [TransformIssue("error", f"Model '{model_name}' not found")])
            _lookup_cache[search_value] = r
            return _result(r[0], *r[1])

        # Validate Field
        try:
            Model._meta.get_field(field_name)
        except FieldDoesNotExist:
            r = (None, [TransformIssue("error", f"Field '{field_name}' not found on model '{model_name}'")])
            _lookup_cache[search_value] = r
            return _result(r[0], *r[1])

        # If value is empty/None, return None (skip lookup)
        if search_value in (None, ""):
            _lookup_cache[search_value] = (None, [])
            return _result(None)

        # Perform Lookup
        try:
            obj = Model.objects.get(**{field_name: search_value})
            r = (obj.pk, [])
        except Model.DoesNotExist:
            r = (None, [TransformIssue("error", f"{model_name} with {field_name}='{search_value}' not found")])
        except Model.MultipleObjectsReturned:
            r = (None, [TransformIssue("error", f"Multiple {model_name} found with {field_name}='{search_value}'")])

        _lookup_cache[search_value] = r
        return _result(r[0], *r[1])

    return name


def static_value_factory(static_value: Any) -> str:
    """
    Return a registered transform name that always returns the same static value,
    ignoring the incoming row data.

    This is useful for populating fields with a constant value across all imported rows.

    Parameters:
      - static_value: the constant value to return for every row

    Usage:
      STATIC_DBCA = static_value_factory("DBCA")
      PIPELINES["organization"] = [STATIC_DBCA]

    Returns the static value for every row.
    """
    key = "static_value_" + hashlib.sha1(str(static_value).encode()).hexdigest()[:8]

    # Check if already registered (cached by value)
    if key in registry._fns:
        return key

    @registry.register(key)
    def inner(value, ctx):
        # Always return the static value, ignore incoming data
        return _result(static_value)

    return key


def taxonomy_lookup(
    group_type_name: str | None = None,
    lookup_field: str = "scientific_name",
    check_previous: bool = True,
    source_key: str | None = None,
):
    """
    Resolve a Taxonomy by lookup_field (e.g. 'scientific_name' or 'taxon_name_id').
    Behaviour:
      - tries cleaned (neutralise_html) then raw value against Taxonomy.{lookup_field}
      - if not found and check_previous=True, looks for a TaxonPreviousName.previous_scientific_name
        matching the value (case-insensitive) and returns that previous_name.taxonomy.pk
    Returns a registered transform name.
    """
    # include source_key in registration key so different caller configs
    # produce distinct registered transforms
    key = f"taxonomy_lookup_{lookup_field}_{int(check_previous)}_{source_key or ''}"

    @registry.register(key)
    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)

        # Prepare candidates: try cleaned first, then raw
        raw = str(value)
        try:
            cleaned = neutralise_html(raw)
        except Exception:
            cleaned = None

        candidates = []
        # Database values will be stored in cleaned form, so prefer the cleaned candidate
        # when available, otherwise use the raw value.
        if cleaned:
            candidates.append(cleaned)
        else:
            candidates.append(raw)

        # Handle trailing ' PN' specially: normalise NBSPs and try pn-stripped
        raw_norm = raw.replace("\u00a0", " ").replace("\u202f", " ")
        if raw_norm.endswith(" PN"):
            pn_raw = raw_norm[:-3].rstrip()
            try:
                cleaned_pn = neutralise_html(pn_raw)
            except Exception:
                cleaned_pn = None
            if cleaned_pn:
                candidates.append(cleaned_pn)
            if cleaned:
                candidates.append(cleaned)
            if pn_raw and pn_raw != raw:
                candidates.append(pn_raw)
        else:
            candidates.append(value)

        # Spacing-normalisation candidate: produce variants that insert a space
        # after a period when followed by a capitalised word (e.g. "sp.Darling" -> "sp. Darling")
        # and ensure there is a space before parentheses ("Range(R..." -> "Range (R...").
        # This helps resolve names like "Baeckea sp.Darling Range(R.J.Cranfield 1673)".
        try:
            norm_variants = []
            for c in list(candidates):
                if not isinstance(c, str):
                    continue
                s = c
                # add space after period when followed by Capital+lowercase (surname or word)
                s = re.sub(r"\.([A-Z][a-z])", r". \1", s)
                # add space before opening parenthesis if missing
                s = re.sub(r"([^\s])\(", r"\1 (", s)
                if s != c and s not in candidates:
                    norm_variants.append(s)
            candidates.extend(norm_variants)
        except Exception:
            # be conservative: if normalization fails, continue without it
            logger.exception("spacing-normalisation in taxonomy_lookup failed")

        # If transform configured for TPFL, add mapping candidate last (final attempt)
        if source_key and source_key.upper() == "TPFL":
            try:
                tpfl_map = _load_tpfl_name_to_nomos()
                if tpfl_map:
                    # Prefer the incoming `value` (this is the common case when
                    # adapters have already mapped raw headers to canonical keys
                    # and the pipeline value contains the NAME content). Fall back
                    # to the original raw row 'NAME' header when present.
                    tpfl_name = None
                    if value not in (None, ""):
                        tpfl_name = value
                    elif isinstance(ctx.row, dict):
                        tpfl_name = ctx.row.get("NAME")

                    if tpfl_name:
                        tpfl_key = str(tpfl_name).replace("\u00a0", " ").replace("\u202f", " ").strip()
                        mapped = tpfl_map.get(tpfl_key)
                        if mapped:
                            try:
                                mapped_clean = neutralise_html(mapped)
                            except Exception:
                                mapped_clean = None
                            if mapped_clean:
                                candidates.append(mapped_clean)
                            candidates.append(mapped)
            except Exception:
                logger.exception("TPFL mapping lookup failed in taxonomy_lookup")

        qs = Taxonomy._default_manager

        # Filter by group type
        if group_type_name:
            try:
                gt = GroupType.objects.get(name__iexact=str(group_type_name).strip())
                qs = qs.filter(kingdom_fk__grouptype=gt)
            except GroupType.DoesNotExist:
                return _result(
                    value,
                    TransformIssue(
                        "error",
                        f"Unknown group_type '{group_type_name}' for Taxonomy lookup",
                    ),
                )

        # Try direct Taxonomy lookup
        for candidate in candidates:
            try:
                obj = qs.get(**{lookup_field: candidate})
                return _result(obj.pk)
            except Taxonomy.DoesNotExist:
                continue
            except Taxonomy.MultipleObjectsReturned:
                try:
                    matches = qs.filter(**{lookup_field: candidate})
                    # If exactly one 'current' record exists among the matches, prefer it.
                    # Use `is_current` field which exists on the Taxonomy model.
                    current_matches = matches.filter(is_current=True)
                    if current_matches.count() == 1:
                        return _result(current_matches.first().pk)
                    # If there are multiple current matches or none, we cannot disambiguate.
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Multiple Taxonomy with {lookup_field}='{value}' found",
                        ),
                    )
                except Exception:
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Multiple Taxonomy with {lookup_field}='{value}' found",
                        ),
                    )

        # Try previous names (case-insensitive match on previous_scientific_name)
        if check_previous:
            for candidate in candidates:
                if isinstance(candidate, str):
                    prev_qs = TaxonPreviousName.objects.filter(previous_scientific_name__iexact=str(candidate).strip())
                else:
                    prev_qs = TaxonPreviousName.objects.filter(previous_name_id=candidate)

                if not prev_qs.exists():
                    continue

                # If multiple previous-name entries exist, allow them when they all
                # point to the same taxonomy_id. Only error when there are
                # conflicting taxonomy links or no linked taxonomy_id at all.
                if prev_qs.count() > 1:
                    # collect taxonomy_id values (may include None)
                    ids = list(prev_qs.values_list("taxonomy_id", flat=True))
                    non_null_ids = {i for i in ids if i is not None}
                    if len(non_null_ids) == 1:
                        # all non-null entries point to the same taxonomy -> accept
                        return _result(next(iter(non_null_ids)))
                    if len(non_null_ids) == 0:
                        return _result(
                            value,
                            TransformIssue(
                                "error",
                                f"TaxonPreviousName for '{value}' found but no linked Taxonomy",
                            ),
                        )
                    # conflicting taxonomy links
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Multiple TaxonPreviousName entries for '{value}' found with different taxonomy links",
                        ),
                    )

                prev = prev_qs.first()
                if prev and prev.taxonomy_id:
                    return _result(prev.taxonomy_id)
                return _result(
                    value,
                    TransformIssue(
                        "error",
                        f"TaxonPreviousName for '{value}' found but no linked Taxonomy",
                    ),
                )

        return _result(
            value,
            TransformIssue("error", f"Taxonomy with {lookup_field}='{value}' not found"),
        )

    # end inner

    return key

    # Lazy-loaded mapping for TPFL NAME -> nomos_canonical_name


_tpfl_name_to_nomos: dict | None = None


def _load_tpfl_name_to_nomos() -> dict:
    """Load TPFL NAME -> nomos_canonical_name mapping from the CSV file.

    Returns a dict where keys are NAME strings (NBSPs normalised and stripped)
    and values are the nomos_canonical_name strings. Errors are logged and an
    empty dict is returned if the CSV cannot be loaded.
    """
    global _tpfl_name_to_nomos
    if _tpfl_name_to_nomos is not None:
        return _tpfl_name_to_nomos

    mapping = {}
    try:
        from django.conf import settings

        base_dir = getattr(settings, "BASE_DIR", os.getcwd())
        csv_path = os.path.join(
            base_dir,
            "private-media",
            "legacy_data",
            "TPFL",
            "TPFL_CS_LISTING_NAME_TO_NOMOS_CANONICAL_NAME.csv",
        )
        if not os.path.exists(csv_path):
            logger.debug("TPFL mapping CSV not found at %s", csv_path)
            _tpfl_name_to_nomos = {}
            return _tpfl_name_to_nomos

        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                name = r.get("NAME")
                nomos = r.get("nomos_canonical_name") or r.get("nomos_taxon_id")
                if not name or not nomos:
                    continue
                # normalise non-breaking spaces and strip
                key = str(name).replace("\u00a0", " ").replace("\u202f", " ").strip()
                mapping[key] = str(nomos).strip()
    except Exception as e:
        logger.exception("Failed to load TPFL NAME->nomos mapping: %s", e)
        mapping = {}

    _tpfl_name_to_nomos = mapping
    return _tpfl_name_to_nomos


# Cache for LegacyTaxonomyMapping list -> {normalized_legacy_canonical_name: {taxonomy_id, taxon_name_id}}
_legacy_taxonomy_mappings_cache: dict[str, dict] = {}


def _norm_legacy_canonical_name(val: str) -> str | None:
    if val is None:
        return None
    s = str(val)
    # Try to use ftfy to fix mojibake/encoding artefacts when available.
    try:
        import ftfy

        try:
            fixed = ftfy.fix_text(s)
            if fixed:
                s = fixed
        except Exception:
            # fall through to conservative heuristics below
            pass
    except Exception:
        # ftfy not installed or import failed; fall back to heuristics below
        pass

    # Attempt to fix common mojibake artifacts where UTF-8 bytes were
    # incorrectly decoded as Latin-1/Windows-1252 (e.g. sequences like
    # 'Ã‚Â' that commonly represent a non-breaking space). Try a best-effort
    # re-decode; on failure fall back to a conservative replacement of
    # the most common artefacts.
    try:
        if "Ã" in s or "Â" in s or "Ã‚" in s:
            try:
                s = s.encode("latin-1").decode("utf-8")
            except Exception:
                # fallback replacements for common mojibake tokens
                s = s.replace("Ã‚Â", " ").replace("Ã‚", " ").replace("Â", " ")
    except Exception:
        # if anything odd happens, continue with best-effort normalisation
        pass

    # normalise non-breaking spaces and narrow NBSPs to plain spaces
    s = s.replace("\u00a0", " ").replace("\u202f", " ")
    try:
        s = neutralise_html(s) or s
    except Exception:
        # best-effort: fall back to the cleaned whitespace form
        pass

    # collapse multiple whitespace (including accidental double-spaces from
    # NBSP substitutions or HTML neutralisation) into a single space, then strip
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _load_legacy_taxonomy_mappings(list_name: str) -> dict:
    """Load and cache LegacyTaxonomyMapping rows for `list_name`.

    Returns a dict keyed by a normalised legacy_canonical_name (and its casefold
    variant) mapping to a dict with keys `taxonomy_id` and `taxon_name_id`.
    """
    if list_name in _legacy_taxonomy_mappings_cache:
        return _legacy_taxonomy_mappings_cache[list_name]

    mapping: dict = {}
    try:
        from boranga.components.main.models import LegacyTaxonomyMapping

        # import Species lazily to optionally resolve species_id (avoid circular import)
        try:
            from boranga.components.species_and_communities.models import Species
        except Exception:
            Species = None

        qs = LegacyTaxonomyMapping.objects.filter(list_name=list_name)

        # Pre-fetch species mapping: taxonomy_id -> species_id
        taxonomy_ids = set(qs.values_list("taxonomy_id", flat=True))
        taxonomy_ids.discard(None)

        tax_to_species = {}
        if Species and taxonomy_ids:
            # Fetch all species linked to these taxonomy_ids
            # Note: Assuming one species per taxonomy_id, or taking one arbitrarily if multiple
            sp_qs = Species.objects.filter(taxonomy_id__in=taxonomy_ids).values("taxonomy_id", "id")
            for sp in sp_qs:
                tax_to_species[sp["taxonomy_id"]] = sp["id"]

        for rec in qs.only("legacy_canonical_name", "taxonomy_id", "taxon_name_id"):
            key = _norm_legacy_canonical_name(rec.legacy_canonical_name)
            if not key:
                continue
            entry = {"taxonomy_id": rec.taxonomy_id, "taxon_name_id": rec.taxon_name_id}
            # If possible, resolve a linked Species id so lookups can be answered
            # from memory without further DB queries during import pipelines.
            if rec.taxonomy_id in tax_to_species:
                entry["species_id"] = tax_to_species[rec.taxonomy_id]

            # store both the canonical key and a casefold variant for case-insensitive lookups
            mapping.setdefault(key, entry)
            kcf = key.casefold()
            mapping.setdefault(kcf, entry)
    except Exception:
        logger.exception("Failed to load LegacyTaxonomyMapping for list %s", list_name)
        mapping = {}

    _legacy_taxonomy_mappings_cache[list_name] = mapping
    return mapping


# Cache for LegacyTaxonomyMapping list -> {legacy_taxon_name_id: {taxonomy_id, taxon_name_id}}
_legacy_taxonomy_id_mappings_cache: dict[str, dict] = {}


def _load_legacy_taxonomy_id_mappings(list_name: str) -> dict:
    """Load and cache LegacyTaxonomyMapping rows for `list_name` keyed by legacy_taxon_name_id.

    Returns a dict keyed by legacy_taxon_name_id mapping to a dict with keys `taxonomy_id` and `taxon_name_id`.
    """
    if list_name in _legacy_taxonomy_id_mappings_cache:
        return _legacy_taxonomy_id_mappings_cache[list_name]

    mapping: dict = {}
    try:
        from boranga.components.main.models import LegacyTaxonomyMapping

        qs = (
            LegacyTaxonomyMapping.objects.filter(list_name=list_name)
            .exclude(legacy_taxon_name_id__isnull=True)
            .exclude(legacy_taxon_name_id="")
        )

        for rec in qs.only("legacy_taxon_name_id", "taxonomy_id", "taxon_name_id"):
            key = str(rec.legacy_taxon_name_id).strip()
            if not key:
                continue
            entry = {"taxonomy_id": rec.taxonomy_id, "taxon_name_id": rec.taxon_name_id}
            mapping[key] = entry
    except Exception:
        logger.exception("Failed to load LegacyTaxonomyMapping (ID) for list %s", list_name)
        mapping = {}

    _legacy_taxonomy_id_mappings_cache[list_name] = mapping
    return mapping


def taxonomy_lookup_legacy_mapping(list_name: str) -> str:
    """Register and return a transform name that resolves a legacy canonical name
    (from `LegacyTaxonomyMapping`) to a taxonomy id.

    Behaviour:
      - normalises incoming value (NBSPs, HTML neutralisation, strip)
      - looks up the normalised key (case-insensitive)
      - returns the `taxonomy_id` if present, else falls back to `taxon_name_id`
      - on missing mapping returns an error TransformIssue
    """
    if not list_name:
        raise ValueError("list_name must be provided")

    key_repr = f"taxonom_lookup_legacy:{list_name}"
    name = "taxonom_lookup_legacy_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(value, ctx: TransformContext):
        if value in (None, ""):
            return _result(None)

        table = _load_legacy_taxonomy_mappings(list_name)
        if not table:
            return _result(
                value,
                TransformIssue(
                    "error",
                    f"No LegacyTaxonomyMapping entries loaded for list '{list_name}'",
                ),
            )

        norm = _norm_legacy_canonical_name(value)
        if norm is None:
            return _result(value, TransformIssue("error", f"Invalid legacy name: {value!r}"))

        entry = table.get(norm) or table.get(norm.casefold())
        if entry:
            # Return the taxonomy id when available, otherwise fall back to taxon_name_id.
            # Do NOT return species_id here — this transform is expected to resolve
            # to a taxonomy/taxon_name identifier (numeric) used by downstream logic.
            tax_id = entry.get("taxonomy_id") or entry.get("taxon_name_id")
            if tax_id is None:
                return _result(
                    value,
                    TransformIssue(
                        "error",
                        f"LegacyTaxonomyMapping for '{value}' has no taxonomy or taxon_name_id",
                    ),
                )
            return _result(tax_id)

        # Fallback: if no legacy mapping found, try resolving the incoming value
        # using the general `taxonomy_lookup` pipeline. This lets us resolve
        # values directly against `Taxonomy` (including previous names) when the
        # legacy table lacks an entry.
        try:
            tax_transform_name = taxonomy_lookup()
            fn = registry._fns.get(tax_transform_name)
            if fn is not None:
                res = fn(value, ctx)
                # Normalize possible return types to TransformResult
                if res is None:
                    return _result(None)
                if isinstance(res, TransformResult):
                    return res
                return _result(res)
        except Exception:
            logger.exception("taxonomy_lookup_legacy_mapping fallback to taxonomy_lookup failed")

        return _result(
            value,
            TransformIssue(
                "error",
                f"LegacyTaxonomyMapping for '{value}' not found in list '{list_name}'",
            ),
        )

    registry._fns[name] = _inner
    return name


def taxonomy_lookup_legacy_mapping_species(list_name: str) -> str:
    """Register and return a transform name that resolves a legacy canonical name
    (from `LegacyTaxonomyMapping`) to a Species id.

    Behaviour:
      - normalises incoming value (NBSPs, HTML neutralisation, strip)
      - looks up the normalised key (case-insensitive)
      - prefers to resolve a linked `taxonomy` -> `Species` and return that `Species.id`
      - if linked `Species` cannot be found, will attempt to resolve via `taxon_name_id`
        (finding the Taxonomy then its Species)
      - on missing mapping or missing species returns an error TransformIssue
    """
    if not list_name:
        raise ValueError("list_name must be provided")

    key_repr = f"taxonom_lookup_legacy_species:{list_name}"
    name = "taxonom_lookup_legacy_species_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(value, ctx: TransformContext):
        if value in (None, ""):
            return _result(None)

        table = _load_legacy_taxonomy_mappings(list_name)
        if not table:
            return _result(
                value,
                TransformIssue(
                    "error",
                    f"No LegacyTaxonomyMapping entries loaded for list '{list_name}'",
                ),
            )

        norm = _norm_legacy_canonical_name(value)
        if norm is None:
            return _result(value, TransformIssue("error", f"Invalid legacy name: {value!r}"))

        entry = table.get(norm) or table.get(norm.casefold())
        if not entry:
            # Fallback: try resolving via the general taxonomy_lookup and then
            # find a linked Species for that taxonomy id. This mirrors the
            # behaviour in `taxonomy_lookup_legacy_mapping` which attempts a
            # Taxonomy lookup when legacy mapping is absent.
            try:
                tax_transform_name = taxonomy_lookup()
                fn = registry._fns.get(tax_transform_name)
                if fn is not None:
                    res = fn(value, ctx)
                    # Normalize to TransformResult-ish handling
                    if res is None:
                        # treat as not found and fall through to error below
                        res_val = None
                    elif isinstance(res, TransformResult):
                        # if the taxonomy lookup returned an error, propagate it
                        if any(i.level == "error" for i in res.issues):
                            return res
                        res_val = res.value
                    else:
                        res_val = res

                    if res_val is not None:
                        try:
                            # attempt to resolve a Species linked to the taxonomy
                            from boranga.components.species_and_communities.models import (
                                Species,
                                Taxonomy,
                            )

                            try:
                                sp = Species.objects.get(taxonomy_id=res_val)
                                return _result(sp.pk)
                            except Species.DoesNotExist:
                                # try resolving via Taxonomy object then Species
                                try:
                                    tax = Taxonomy.all_objects.get(pk=res_val)
                                    sp = Species.objects.get(taxonomy=tax)
                                    return _result(sp.pk)
                                except Exception:
                                    # fall through to final error return
                                    pass
                            except Species.MultipleObjectsReturned:
                                return _result(
                                    value,
                                    TransformIssue(
                                        "error",
                                        f"Multiple Species linked to taxonomy_id={res_val} for '{value}'",
                                    ),
                                )
                        except Exception:
                            logger.exception("taxonomy_lookup_legacy_mapping_species fallback lookup failed")
            except Exception:
                logger.exception("taxonomy_lookup_legacy_mapping_species fallback to taxonomy_lookup failed")

            return _result(
                value,
                TransformIssue(
                    "error",
                    f"LegacyTaxonomyMapping for '{value}' not found in list '{list_name}'",
                ),
            )

        # Prefer a cached species_id if present (no DB query)
        species_id = entry.get("species_id")
        if species_id is not None:
            return _result(species_id)

        # Otherwise fall back to attempting to resolve via Taxonomy/TaxonName
        tax_id = entry.get("taxonomy_id")
        tnid = entry.get("taxon_name_id")

        try:
            # local imports to avoid circular import issues
            from boranga.components.species_and_communities.models import (
                Species,
                Taxonomy,
            )

            if tax_id:
                try:
                    sp = Species.objects.get(taxonomy_id=tax_id)
                    return _result(sp.pk)
                except Species.DoesNotExist:
                    # fall through to try via taxon_name_id
                    pass
                except Species.MultipleObjectsReturned:
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Multiple Species linked to taxonomy_id={tax_id} for '{value}'",
                        ),
                    )

            if tnid:
                try:
                    tax = Taxonomy.all_objects.get(taxon_name_id=tnid)
                except Taxonomy.DoesNotExist:
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Taxonomy with taxon_name_id={tnid} not found for '{value}'",
                        ),
                    )
                except Taxonomy.MultipleObjectsReturned:
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Multiple Taxonomy entries with taxon_name_id={tnid} found for '{value}'",
                        ),
                    )

                try:
                    sp = Species.objects.get(taxonomy=tax)
                    return _result(sp.pk)
                except Species.DoesNotExist:
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"LegacyTaxonomyMapping for '{value}' maps to taxonomy "
                            f"(taxon_name_id={tnid}) but no Species found",
                        ),
                    )
                except Species.MultipleObjectsReturned:
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Multiple Species linked to taxon_name_id={tnid} for '{value}'",
                        ),
                    )

            # If we reach here, we had a mapping but could not resolve to a Species
            return _result(
                value,
                TransformIssue(
                    "error",
                    f"LegacyTaxonomyMapping for '{value}' has no linked Species for list '{list_name}'",
                ),
            )

        except Exception as e:
            logger.exception("taxonomy_lookup_legacy_mapping_species lookup failed: %s", e)
            return _result(value, TransformIssue("error", f"lookup error: {e}"))

    registry._fns[name] = _inner
    return name


def taxonomy_lookup_legacy_id_mapping(list_name: str) -> str:
    """Register and return a transform name that resolves a legacy taxon name id
    (from `LegacyTaxonomyMapping`) to a taxonomy_id.

    Behaviour:
      - looks up the legacy_taxon_name_id
      - returns the `taxonomy_id` if present
      - on missing mapping returns an error TransformIssue
    """
    if not list_name:
        raise ValueError("list_name must be provided")

    key_repr = f"taxonom_lookup_legacy_id:{list_name}"
    name = "taxonom_lookup_legacy_id_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(value, ctx: TransformContext):
        if value in (None, ""):
            return _result(None)

        table = _load_legacy_taxonomy_id_mappings(list_name)
        if not table:
            return _result(
                value,
                TransformIssue(
                    "error",
                    f"No LegacyTaxonomyMapping entries loaded for list '{list_name}'",
                ),
            )

        key = str(value).strip()
        entry = table.get(key)
        if entry:
            # Return the taxonomy_id
            tax_id = entry.get("taxonomy_id")
            if tax_id is None:
                return _result(
                    value,
                    TransformIssue(
                        "error",
                        f"LegacyTaxonomyMapping for ID '{value}' has no taxonomy_id",
                    ),
                )
            return _result(tax_id)

        return _result(
            value,
            TransformIssue(
                "error",
                f"LegacyTaxonomyMapping for ID '{value}' not found in list '{list_name}'",
            ),
        )

    registry._fns[name] = _inner
    return name


@registry.register("upper")
def t_upper(value, ctx):
    return _result(value.upper() if isinstance(value, str) else value)


@registry.register("lower")
def t_lower(value, ctx):
    return _result(value.lower() if isinstance(value, str) else value)


@registry.register("title_case")
def t_title(value, ctx):
    return _result(value.title() if isinstance(value, str) else value)


@registry.register("cap_length_50")
def t_cap_len_50(value, ctx):
    if value is None:
        return _result(None)
    s = str(value)
    if len(s) <= 50:
        return _result(s)
    return _result(s[:50], TransformIssue("warning", "Truncated to 50 chars"))


@registry.register("group_type_by_name")
def t_group_type_by_name(value, ctx):
    if value in (None, ""):
        return TransformResult(value=None, issues=[TransformIssue("error", "group_type required")])
    try:
        gt = GroupType.objects.get(name__iexact=str(value).strip())
        return _result(gt.id)
    except GroupType.DoesNotExist:
        return TransformResult(
            value=None,
            issues=[TransformIssue("error", f"Unknown group_type '{value}'")],
        )


@registry.register("emailuser_by_email")
def t_emailuser_by_email(value, ctx):
    if value in (None, ""):
        return _result(None)
    email = str(value).strip()
    try:
        user = EmailUserRO.objects.get(email__iexact=email)
        return _result(user.id)
    except EmailUserRO.DoesNotExist:
        return _result(value, TransformIssue("error", f"User with email='{value}' not found"))
    except EmailUserRO.MultipleObjectsReturned:
        return _result(value, TransformIssue("error", f"Multiple users with email='{value}'"))


def emailuser_by_legacy_username_factory(legacy_system: str) -> str:
    """
    Return a registered transform name that resolves legacy username -> emailuser_id
    bound to the provided legacy_system (must be specified).

    Usage:
      EMAILUSER_TPFL = emailuser_by_legacy_username_factory("TPFL")
      PIPELINES["owner_id"] = [EMAILUSER_TPFL]
    """
    if not legacy_system:
        raise ValueError("legacy_system must be provided to bind this transform")

    key = f"emailuser_by_legacy_username:{legacy_system}"
    name = "emailuser_by_legacy_username_" + hashlib.sha1(key.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    # Cache lookup results to avoid repeated DB queries
    # Key: username value, Value: (result_value, list[TransformIssue])
    _lookup_cache = {}

    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)

        if value in _lookup_cache:
            res_val, res_issues = _lookup_cache[value]
            return _result(res_val, *res_issues)

        username = str(value).strip()
        qs = LegacyUsernameEmailuserMapping.objects.filter(
            legacy_system__iexact=str(legacy_system), legacy_username__iexact=username
        )
        try:
            mapping = qs.get()
            res_val, res_issues = mapping.emailuser_id, []
            _lookup_cache[value] = (res_val, res_issues)
            return _result(res_val, *res_issues)
        except LegacyUsernameEmailuserMapping.DoesNotExist:
            res_val = value
            res_issues = [
                TransformIssue(
                    "error",
                    f"User with legacy username='{value}' not found for system='{legacy_system}'",
                )
            ]
            _lookup_cache[value] = (res_val, res_issues)
            return _result(res_val, *res_issues)
        except LegacyUsernameEmailuserMapping.MultipleObjectsReturned:
            res_val = value
            res_issues = [
                TransformIssue(
                    "error",
                    f"Multiple users with legacy username='{value}' for system='{legacy_system}'",
                )
            ]
            _lookup_cache[value] = (res_val, res_issues)
            return _result(res_val, *res_issues)

    registry._fns[name] = inner
    return name


def emailuser_object_by_legacy_username_factory(legacy_system: str) -> str:
    """
    Return a registered transform name that resolves legacy username -> EmailUser object
    (the full EmailUser model instance, not just the ID).

    This is useful when you need to extract attributes from the EmailUser object
    using pluck_attribute_factory, e.g., to get the full name.

    Results are cached for the duration of the data migration to avoid repeated
    database queries for the same legacy username.

    Usage:
      EMAILUSER_OBJ_TPFL = emailuser_object_by_legacy_username_factory("TPFL")
      SUBMITTER_NAME = pluck_attribute_factory("get_full_name")
      PIPELINES["submitter_information_name"] = [EMAILUSER_OBJ_TPFL, SUBMITTER_NAME]
    """
    if not legacy_system:
        raise ValueError("legacy_system must be provided to bind this transform")

    key = f"emailuser_object_by_legacy_username:{legacy_system}"
    name = "emailuser_object_by_legacy_username_" + hashlib.sha1(key.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    # Cache for EmailUser objects keyed by (legacy_system, username)
    cache: dict = {}

    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)
        username = str(value).strip()
        cache_key = (str(legacy_system).lower(), username.lower())

        # Return cached result if available
        if cache_key in cache:
            cached = cache[cache_key]
            if isinstance(cached, TransformResult):
                return cached
            return _result(cached)

        qs = LegacyUsernameEmailuserMapping.objects.filter(
            legacy_system__iexact=str(legacy_system), legacy_username__iexact=username
        )
        try:
            mapping = qs.get()
            # Fetch and return the actual EmailUser object
            email_user = EmailUserRO.objects.get(id=mapping.emailuser_id)
            result = _result(email_user)
            cache[cache_key] = result
            return result
        except LegacyUsernameEmailuserMapping.DoesNotExist:
            result = _result(
                value,
                TransformIssue(
                    "error",
                    f"User with legacy username='{value}' not found for system='{legacy_system}'",
                ),
            )
            cache[cache_key] = result
            return result
        except LegacyUsernameEmailuserMapping.MultipleObjectsReturned:
            result = _result(
                value,
                TransformIssue(
                    "error",
                    f"Multiple users with legacy username='{value}' for system='{legacy_system}'",
                ),
            )
            cache[cache_key] = result
            return result
        except EmailUserRO.DoesNotExist:
            result = _result(
                value,
                TransformIssue(
                    "error",
                    f"EmailUser with id={mapping.emailuser_id} not found for legacy username='{value}'",
                ),
            )
            cache[cache_key] = result
            return result

    registry._fns[name] = inner
    return name


@registry.register("split_multiselect")
def t_split_multiselect(value, ctx):
    """Split a multi-select cell into a trimmed, deduped list (split on ';' or ',')."""
    if value in (None, ""):
        return _result(None)
    s = str(value)
    parts = [p.strip() for p in re.split(r"[;,]", s) if p.strip()]
    seen = set()
    out = []
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return _result(out)


# map spreadsheet datum values -> source CRS
_DATUM_CRS = {
    "GDA94": "EPSG:4283",
    "AGD84": "EPSG:4283",
    "WGS84": "EPSG:4326",
    "GPS": "EPSG:4326",
    "UNKNOWN": "EPSG:4326",
}

# single continental projection to use for metric buffering
_ALBERS_EPSG = "EPSG:3577"  # GDA94 / Australian Albers


def point_to_circle_factory(
    lat_col: str = "LAT",
    lon_col: str = "LON",
    datum_col: str = "DATUM",
    diameter_m: float = 1.0,
    buffer_resolution: int = 16,
    return_wkt: bool = False,
):
    """
    Returns a transform function (value, ctx) -> TransformResult(GEOSGeometry|WKT|None, issues).

    - The returned function ignores 'value' and reads the row from ctx (ctx.row or ctx['row']).
    - If called with no args the factory defaults to lat_col='LAT' and lon_col='LON'.
    - datum values UNKNOWN/GPS -> WGS84 by default.
    - Buffers using a single continental projected CRS (EPSG:3577) for meter units.
    """
    radius = float(diameter_m) / 2.0

    def transform_fn(value, ctx):
        # support both TransformContext objects and plain dict ctx
        row = None
        if ctx is None:
            logger.warning("point_to_circle: missing transform context for value=%r", value)
            return _result(
                None,
                TransformIssue("warning", "No valid coordinates (missing context)"),
            )
        # ctx may be an object with .row attribute (TransformContext) or a dict
        row = getattr(ctx, "row", None) or (ctx.get("row") if isinstance(ctx, dict) else None)
        if not row:
            logger.warning("point_to_circle: missing row in context for value=%r", value)
            return _result(None, TransformIssue("warning", "No valid coordinates (missing row)"))

        try:
            raw_lat = row.get(lat_col)
            raw_lon = row.get(lon_col)
            raw_datum = row.get(datum_col, "UNKNOWN")
            if raw_lat in (None, "") or raw_lon in (None, ""):
                logger.warning(
                    "point_to_circle: no valid lat/lon (lat=%r lon=%r) for row: %r",
                    raw_lat,
                    raw_lon,
                    # keep row small in logs
                    {k: row.get(k) for k in (lat_col, lon_col, datum_col) if k in row},
                )
                return _result(None, TransformIssue("warning", "No valid coordinates"))

            lat = float(raw_lat)
            lon = float(raw_lon)
            datum = (raw_datum or "UNKNOWN").strip().upper()
            src_crs = _DATUM_CRS.get(datum, "EPSG:4326")

            # normalize to EPSG:4326 lon/lat for consistent processing
            if src_crs != "EPSG:4326":
                to_4326 = Transformer.from_crs(src_crs, "EPSG:4326", always_xy=True)
                lon_x, lat_y = to_4326.transform(lon, lat)
            else:
                lon_x, lat_y = lon, lat

            # project to Albers (meters), buffer, then back to 4326
            to_albers = Transformer.from_crs("EPSG:4326", _ALBERS_EPSG, always_xy=True)
            from_albers = Transformer.from_crs(_ALBERS_EPSG, "EPSG:4326", always_xy=True)

            pt_geo = Point(lon_x, lat_y)
            pt_alb = shapely_transform(lambda x, y: to_albers.transform(x, y), pt_geo)
            circ_alb = pt_alb.buffer(radius, resolution=buffer_resolution)
            circ_4326 = shapely_transform(lambda x, y: from_albers.transform(x, y), circ_alb)

            geom = GEOSGeometry(circ_4326.wkt, srid=4326)
            return _result(geom.wkt if return_wkt else geom)

        except Exception as e:
            logger.exception("point_to_circle transform failed for row: %r", row)
            return _result(value, TransformIssue("error", f"point_to_circle error: {e}"))

    return transform_fn


# register a default no-arg transform for convenience
try:
    TRANSFORMS  # type: ignore[name-defined]
except NameError:
    TRANSFORMS = {}

TRANSFORMS["point_to_1m_circle"] = point_to_circle_factory()


def validate_multiselect(choice_transform_name: str):
    """
    Factory returning a transform name that applies a single-item transform to
    every element in a list and returns the normalized list (or issues).
    """
    h = hashlib.sha1(choice_transform_name.encode()).hexdigest()[:8]
    name = f"validate_multiselect_{h}"
    if name in registry._fns:
        return name

    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)
        if not isinstance(value, list | tuple):
            return _result(value, TransformIssue("error", "Expected list for multiselect"))
        item_fn = registry._fns.get(choice_transform_name)
        if item_fn is None:
            return _result(
                value,
                TransformIssue("error", f"Unknown transform '{choice_transform_name}'"),
            )

        normalized = []
        issues = []
        for item in value:
            res = item_fn(item, ctx)
            normalized.append(res.value)
            issues.extend(res.issues)
        return TransformResult(normalized, issues)

    registry._fns[name] = inner
    return name


def normalize_delimited_list_factory(delimiter: str = ";", suffix: str | None = None):
    """
    Register and return a transform name that normalises a delimited list:
    - splits on the exact delimiter,
    - trims each item,
    - removes empty items,
    - if no items remain -> returns None
    - if one item remains -> returns that item (string)
    - otherwise -> returns the items joined by the delimiter (string)

    Optional suffix: appended after the delimiter when joining (e.g. suffix=" " will join as "; ").
    Usage:
      NORM_SEMI = normalize_delimited_list_factory(";", None)    # "a;b"
      NORM_SEMI_SP = normalize_delimited_list_factory(";", " ")  # "a; b"
      pipeline = ["strip", "blank_to_none", NORM_SEMI_SP]
    """
    key = f"normalize_delimited_list:{delimiter}:{suffix or ''}"
    name = "normalize_" + hashlib.sha1(key.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    joiner = delimiter + (suffix or "")

    def _inner(value, ctx):
        # treat None/empty as None
        if value in (None, ""):
            return _result(None)

        s = str(value)
        parts = [p.strip() for p in s.split(delimiter)]
        # keep only non-empty items
        items = [p for p in parts if p != ""]

        if not items:
            return _result(None)
        if len(items) == 1:
            return _result(items[0])
        return _result(joiner.join(items))

    registry._fns[name] = _inner
    return name


def csv_lookup_factory(
    key_column: str,
    value_column: str,
    csv_filename: str,
    legacy_system: str | None = None,
    path: str | None = None,
    *,
    default=None,
    required: bool = False,
    case_insensitive: bool = True,
    delimiter: str = ",",
    use_cache: bool = True,
    cache_timeout: int = 3600,
) -> str:
    """
    Return a registered transform name that looks up a value in a CSV file
    and returns the corresponding value from another column.

    Parameters:
      - csv_filename: filename of the CSV (e.g. "DRF_LOV_RECORD_SOURCE_VWS.csv")
      - legacy_system: legacy system name (e.g. "TPFL") to add to the path
      - path: optional directory or full path to CSV; if omitted the loader will
              search the default legacy_data location (dm_mappings.load_csv_mapping)
      - key_column / value_column: header names in the CSV
    """
    if not key_column or not value_column or not csv_filename:
        raise ValueError("key_column, value_column and csv_filename must be provided")

    key_repr = (
        f"csv_lookup:{csv_filename}:{key_column}:{value_column}:"
        f"legacy_system={legacy_system}:{path or ''}:{case_insensitive}:{delimiter}"
    )
    name = "csv_lookup_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(value, ctx: TransformContext):
        if value in (None, ""):
            return _result(default)

        # Use mappings loader which will resolve default location if path is None
        mapping, resolved_path = dm_mappings.load_csv_mapping(
            csv_filename,
            key_column,
            value_column,
            legacy_system=legacy_system,
            path=path,
            delimiter=delimiter,
            case_insensitive=case_insensitive,
        )

        # Use cache keyed by resolved path + params
        cache_key = (
            "csv_lookup_map:" + hashlib.sha1(f"{resolved_path}:{case_insensitive}:{delimiter}".encode()).hexdigest()
        )
        if use_cache:
            cached = cache.get(cache_key)
            if cached is None and mapping is not None:
                cache.set(cache_key, mapping, cache_timeout)
            elif cached is not None:
                mapping = cached

        if mapping is None:
            msg = f"CSV mapping file not usable: {resolved_path}"
            if required:
                return _result(value, TransformIssue("error", msg))
            return _result(default, TransformIssue("warning", msg))

        key = str(value).strip()
        lookup_key = key.casefold() if case_insensitive else key
        mapped = mapping.get(lookup_key)
        if mapped is not None:
            return _result(mapped)

        # fallback raw key check
        if key in mapping:
            return _result(mapping[key])

        msg = f"Value '{value}' not found in mapping file {resolved_path}"
        if required:
            return _result(value, TransformIssue("error", msg))
        return _result(default)

    registry._fns[name] = _inner
    return name


# ------------------------ End Common Transform Functions ------------------------


def run_pipeline(pipeline: list[TransformFn], value: Any, ctx: TransformContext) -> TransformResult:
    current = TransformResult(value)
    for fn in pipeline:
        step_res = fn(current.value, ctx)
        # Some transform callables may return raw values or None instead of
        # a TransformResult. Normalize to TransformResult for safety.
        if step_res is None:
            step_res = TransformResult(None, [])
        elif not isinstance(step_res, TransformResult):
            # wrap raw return value with empty issues
            step_res = TransformResult(step_res, [])

        # Accumulate issues
        current = TransformResult(step_res.value, current.issues + step_res.issues)
        # Stop further transforms on error (optional policy)
        if any(i.level == "error" for i in step_res.issues):
            break
    return current


# This is an example only, not to be used
# Example column mapping: (sheet, column_header) -> list of transform names
COLUMN_PIPELINES: dict[tuple[str, str], list[str]] = {
    ("occurrence", "Species Code"): ["strip", "required", "upper"],
    ("occurrence", "Count"): ["strip", "blank_to_none", "to_int"],
    ("occurrence", "Observed Date"): ["strip", "blank_to_none", "date_iso"],
}

# ---------------------------------------------------------------------------
# Importer registry (handlers) with autodiscovery
# ---------------------------------------------------------------------------


@dataclass
class ImportContext:
    dry_run: bool
    user_id: int | None = None
    stats: dict = None
    limit: int | None = None
    migration_run: MigrationRun | None = None

    def __post_init__(self):
        if self.stats is None:
            self.stats = {}


class BaseSheetImporter:
    slug: str = ""  # unique key, e.g. "occurrence"
    sheet_name: str | None = None
    description: str = ""

    def new_stats(self):
        return {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "warnings": 0,
        }

    def add_arguments(self, parser):
        pass

    def run(self, path: str, ctx: ImportContext, **options):
        raise NotImplementedError

    def clear_targets(self, ctx: ImportContext, include_children: bool = False, **options):
        """
        Optional hook to delete target model data prior to running an importer.

        - `include_children`: when True (used for runmany wipe), implementations may also
          delete dependent/child tables. Default False for single-run wipe.
        - `ctx.dry_run` should be respected by implementations (no-op on dry-run).

        Default implementation is a no-op (importer may override).
        """
        return None


# Internal storage
_importer_registry: dict[str, type[BaseSheetImporter]] = {}
_discovered = False


def register(importer_cls: type[BaseSheetImporter]):
    """Decorator to register a sheet importer."""
    if not importer_cls.slug:
        raise ValueError("Importer class must define a non-empty 'slug'")
    _importer_registry[importer_cls.slug] = importer_cls
    return importer_cls


def _autodiscover():
    global _discovered
    if _discovered:
        return
    try:
        import importlib
        import pkgutil

        from . import handlers
    except ModuleNotFoundError:
        _discovered = True
        return
    prefix = handlers.__name__ + "."
    for modinfo in pkgutil.iter_modules(handlers.__path__, prefix):
        base = modinfo.name.rsplit(".", 1)[-1]
        if base.startswith("_"):
            continue
        importlib.import_module(modinfo.name)
    _discovered = True


def all_importers():
    _autodiscover()
    # Deterministic order by slug
    return [_importer_registry[k] for k in sorted(_importer_registry.keys())]


def get(slug: str) -> type[BaseSheetImporter]:
    _autodiscover()
    try:
        return _importer_registry[slug]
    except KeyError:
        raise KeyError(f"Importer slug '{slug}' not found. Available: {', '.join(sorted(_importer_registry))}")


def dependent_from_column_factory(
    dependency_column: str,
    mapping: dict | None = None,
    mapper: Callable[[Any, TransformContext], Any] | None = None,
    default=None,
    error_on_unknown: bool = False,
    warning_on_unknown: bool = False,
) -> str:
    """
    Return a registered transform name that derives its output from another column
    in the same raw row (ctx.row). `mapping` may be:
      - a dict for direct lookups,
      - a registered transform name (str) to apply to the dependency value,
      - or a callable taking (dep, ctx) returning a value or TransformResult.
    If `mapper` is provided it takes precedence.
    """
    if not dependency_column:
        raise ValueError("dependency_column must be provided")

    key_repr = f"{dependency_column}:{mapping!r}:{bool(mapper)}:{default!r}:{error_on_unknown}:{warning_on_unknown}"
    name = "dependent_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(value, ctx: TransformContext):
        dep = ctx.row.get(dependency_column) if ctx and isinstance(ctx.row, dict) else None

        # mapper supplied -> call it
        if mapper is not None:
            try:
                out = mapper(dep, ctx)
                return _result(out)
            except Exception as e:
                return _result(value, TransformIssue("error", f"mapper error: {e}"))

        # If mapping is a registered transform name (str) or a callable, apply it
        if mapping is not None:
            # registered transform name
            if isinstance(mapping, str):
                fn = registry._fns.get(mapping)
                if fn is None:
                    return _result(value, TransformIssue("error", f"Unknown transform '{mapping}'"))
                try:
                    res = fn(dep, ctx)
                    if isinstance(res, TransformResult):
                        return res
                    return _result(res)
                except Exception as e:
                    return _result(value, TransformIssue("error", f"mapping transform error: {e}"))

            # callable mapping
            if callable(mapping):
                try:
                    res = mapping(dep, ctx)
                    if isinstance(res, TransformResult):
                        return res
                    return _result(res)
                except Exception as e:
                    return _result(value, TransformIssue("error", f"mapping callable error: {e}"))

            # mapping dict behaviour (fallback)
            try:
                if dep in mapping:
                    return _result(mapping[dep])
                # try string-normalised lookup for convenience
                k = str(dep).strip()
                if k in mapping:
                    return _result(mapping[k])
            except Exception:
                pass
            if error_on_unknown:
                return _result(
                    value,
                    TransformIssue("error", f"Unknown value for {dependency_column}: {dep!r}"),
                )
            if warning_on_unknown:
                return _result(
                    value,
                    TransformIssue("warning", f"Unknown value for {dependency_column}: {dep!r}"),
                )
            return _result(default)

        # no mapping or mapper -> return the dependency value (or default if absent)
        return _result(dep if dep is not None else default)

    registry._fns[name] = _inner
    return name


def dependent_apply_transform_factory(dependency_column: str, transform_name: str) -> str:
    """
    Return a registered transform name which takes the value from `dependency_column`
    in the raw row and applies the already-registered transform `transform_name`
    to that value.

    Example:
      FK_ON_ALT = dependent_apply_transform_factory("ALT_SPNAME", "fk_app_label.modelname_taxonomy_id")
      PIPELINES["species_id"] = ["strip", "blank_to_none", FK_ON_ALT]
    """
    if not dependency_column:
        raise ValueError("dependency_column must be provided")
    if not transform_name:
        raise ValueError("transform_name must be provided")

    key_repr = f"dep_apply:{dependency_column}:{transform_name}"
    name = "dep_apply_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(_value, ctx: TransformContext):
        dep = ctx.row.get(dependency_column) if ctx and isinstance(ctx.row, dict) else None

        fn = registry._fns.get(transform_name)
        if fn is None:
            return _result(
                _value,
                TransformIssue("error", f"Unknown transform '{transform_name}'"),
            )

        try:
            # Apply the target transform to the dependency value and return its result.
            res = fn(dep, ctx)
            # Ensure we return a TransformResult instance (preserve issues/value)
            if isinstance(res, TransformResult):
                return res
            # Fallback: wrap raw return values
            return _result(res)
        except Exception as e:
            return _result(_value, TransformIssue("error", f"transform error: {e}"))

    registry._fns[name] = _inner
    return name


def pluck_attribute_factory(attr: str, default=None) -> str:
    """
    Return a transform name that takes the incoming value (often a model instance)
    and returns getattr(value, attr, default).
    """
    if not attr:
        raise ValueError("attr must be provided")
    name = "pluck_" + attr
    if name in registry._fns:
        return name

    def _inner(value, ctx: TransformContext):
        try:
            if value is None:
                return _result(default)
            if hasattr(value, attr):
                return _result(getattr(value, attr))
            # support dict-like returns
            if isinstance(value, dict) and attr in value:
                return _result(value[attr])
            return _result(
                default,
                TransformIssue("warning", f"attribute {attr!r} not found on value"),
            )
        except Exception as e:
            return _result(default, TransformIssue("error", f"pluck attribute error: {e}"))

    registry._fns[name] = _inner
    return name


def build_legacy_map_transform(
    legacy_system: str,
    list_name: str,
    *,
    required: bool = True,
    return_type: Literal["id", "canonical", "both"] = "id",
) -> str:
    """
    Register and return a transform name that maps legacy enumerated values
    via the mappings.preload_map / dm_mappings._CACHE mechanism.
    """
    key = f"legacy_map:{legacy_system}:{list_name}:{return_type}"
    name = "legacy_map_" + hashlib.sha1(key.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def fn(value, ctx):
        if value in (None, ""):
            if required:
                return TransformResult(
                    value=None,
                    issues=[TransformIssue("error", f"{list_name} required")],
                )
            return _result(None)

        # ensure mapping loaded
        dm_mappings.preload_map(legacy_system, list_name)
        table = dm_mappings._CACHE.get((legacy_system, list_name), {})
        norm = dm_mappings._norm(value)
        if norm not in table:
            return TransformResult(
                value=None,
                issues=[TransformIssue("error", f"Unmapped {legacy_system}.{list_name} value '{value}'")],
            )
        entry = table[norm]
        canonical = entry.get("canonical") or entry.get("raw")
        # sentinel ignores
        canonical_norm = str(canonical).strip().casefold()
        if canonical_norm in {dm_mappings.IGNORE_SENTINEL.casefold(), "ignore"}:
            ctx.stats.setdefault("ignored_legacy_values", []).append(
                {"system": legacy_system, "list": list_name, "value": value}
            )
            return _result(
                None,
                TransformIssue(
                    "info",
                    f"Legacy {legacy_system}.{list_name} value '{value}' intentionally ignored",
                ),
            )

        if return_type == "id":
            return _result(entry.get("target_id"))
        if return_type == "canonical":
            return _result(canonical)
        if return_type == "both":
            return _result((entry.get("target_id"), canonical))
        return _result(entry.get("target_id"))

    registry._fns[name] = fn
    return name


@registry.register("format_date_dmy")
def t_format_date_dmy(value, ctx):
    """
    Accept a date or datetime and return "dd/mm/YYYY" string.
    Expect this to be used after date_from_datetime_iso (so value is a date).
    """
    if value in (None, ""):
        return _result(None)
    # value may be date or datetime or string
    try:
        if isinstance(value, datetime):
            d = value.date()
        elif isinstance(value, date):
            d = value
        else:
            # try to parse forgivingly using existing date_from_datetime_iso logic:
            res = registry._fns.get("date_from_datetime_iso")
            if res is not None:
                parsed = res(value, ctx)
                if parsed and parsed.value:
                    d = parsed.value
                else:
                    return _result(
                        value,
                        TransformIssue("error", f"Unrecognized date for formatting: {value!r}"),
                    )
            else:
                return _result(
                    value,
                    TransformIssue("error", f"No parser available for date value: {value!r}"),
                )
        return _result(d.strftime("%d/%m/%Y"))
    except Exception as e:
        return _result(value, TransformIssue("error", f"format_date_dmy error: {e}"))


def to_int_trailing_factory(prefix: str, required: bool = False):
    """
    Build a transform that extracts trailing integer digits only when the input
    string begins with `prefix`. If required=True and the prefix is missing
    or no trailing digits found, emit an error TransformIssue.
    """
    pref = str(prefix or "")

    def _transform(value, ctx):
        if value in (None, ""):
            return _result(None)
        s = str(value).strip()
        # must start with prefix
        if not s.startswith(pref):
            if required:
                return _result(value, TransformIssue("error", f"expected prefix {pref!r}"))
            return _result(None)
        m = re.search(r"(" + r"\d+" + r")\s*$", s)
        if not m:
            if required:
                return _result(
                    value,
                    TransformIssue("error", f"no trailing digits after prefix {pref!r}"),
                )
            return _result(None)
        try:
            return _result(int(m.group(1)))
        except Exception as e:
            return _result(value, TransformIssue("error", f"to_int_trailing failed: {e}"))

    return _transform


def conditional_transform_factory(
    condition_column: str,
    condition_value: Any,
    true_column: str,
    true_transform: str | None = None,
    false_value: Any = None,
) -> str:
    """
    Return a registered transform name that applies conditional logic:
    - If condition_column equals condition_value, apply true_transform to true_column value
    - Otherwise return false_value

    Parameters:
      - condition_column: column name to check in the row
      - condition_value: value to compare against (uses == comparison)
      - true_column: column name to use when condition is true
      - true_transform: registered transform name to apply to true_column value (optional)
      - false_value: value to return when condition is false (default None)

    Usage:
      APPROVED_BY_IF_ACCEPTED = conditional_transform_factory(
          condition_column="processing_status",
          condition_value="PROCESSING_STATUS_APPROVED",
          true_column="modified_by",
          true_transform="emailuser_by_legacy_username_tpfl"
      )
      PIPELINES["approved_by"] = [APPROVED_BY_IF_ACCEPTED]
    """
    if not condition_column or not true_column:
        raise ValueError("condition_column and true_column must be provided")

    key_repr = f"conditional:{condition_column}:{condition_value}:{true_column}:{true_transform or ''}:{false_value!r}"
    name = "conditional_" + hashlib.sha1(key_repr.encode()).hexdigest()[:8]
    if name in registry._fns:
        return name

    def _inner(value, ctx: TransformContext):
        # Get condition and true column values from row
        row = ctx.row if ctx and isinstance(ctx.row, dict) else {}
        condition_val = row.get(condition_column)
        true_val = row.get(true_column)

        # Check condition
        if condition_val != condition_value:
            return _result(false_value)

        # If no true_column value, return false_value
        if true_val in (None, ""):
            return _result(false_value)

        # If no transform specified, return the value as-is
        if true_transform is None:
            return _result(true_val)

        # Apply the registered transform to the true_column value
        fn = registry._fns.get(true_transform)
        if fn is None:
            return _result(
                value,
                TransformIssue("error", f"Unknown transform '{true_transform}'"),
            )

        try:
            res = fn(true_val, ctx)
            if isinstance(res, TransformResult):
                return res
            return _result(res)
        except Exception as e:
            return _result(value, TransformIssue("error", f"conditional transform error: {e}"))

    registry._fns[name] = _inner
    return name


def region_from_district_factory():
    """
    Factory that returns a registered transform name for deriving region_id from district_id.

    This transform looks up a District by its ID and extracts the associated region_id.
    Results are cached for the duration of the migration run to avoid repeated DB lookups.

    IMPORTANT: This transform is designed to work with the raw CSV row and extracts the
    OCRLocation__district value from the row context, applies the district transform to it,
    then derives the region from the transformed district ID.

    Usage:
      REGION_FROM_DISTRICT_TRANSFORM = region_from_district_factory()
      PIPELINES["OCRLocation__region"] = [REGION_FROM_DISTRICT_TRANSFORM]

    The transform ignores the input value (OCRLocation__region from CSV) and instead derives
    the region from the OCRLocation__district in the same row. Returns None if district is
    not found or cannot be transformed.
    """
    key = "region_from_district"
    name = "region_from_district_" + hashlib.sha1(key.encode()).hexdigest()[:8]

    if name in registry._fns:
        return name

    # Create a cache dict that persists for the migration run
    district_to_region_cache = {}

    def inner(value, ctx: TransformContext):
        # Ignore the input value (OCRLocation__region from CSV, which is usually empty)
        # Instead, derive region from OCRLocation__district in the row
        if not ctx or not isinstance(ctx.row, dict):
            return _result(None)

        # Get the raw district value from the row (this will be a legacy code like "5", "67")
        raw_district_value = ctx.row.get("OCRLocation__district")
        if raw_district_value is None or raw_district_value == "":
            return _result(None)

        # Derive region from the raw district value by looking it up in LegacyValueMap
        try:
            from django.contrib.contenttypes.models import ContentType

            from boranga.components.main.models import LegacyValueMap
            from boranga.components.species_and_communities.models import District

            # Get the District content type and look up the legacy mapping
            district_ct = ContentType.objects.get_for_model(District)
            lvm = LegacyValueMap.objects.filter(
                legacy_system="TPFL",
                list_name="DISTRICT (DRF_LOV_DEC_DISTRICT_VWS)",
                legacy_value=str(raw_district_value).strip(),
                target_content_type=district_ct,
                active=True,
            ).first()

            if lvm:
                district_id = lvm.target_object_id
                # Check cache
                if district_id in district_to_region_cache:
                    return _result(district_to_region_cache[district_id])

                # Look up district and extract region_id
                district = District.objects.get(pk=district_id)
                region_id = district.region_id
                # Cache the result
                district_to_region_cache[district_id] = region_id
                return _result(region_id)
            else:
                return _result(
                    None,
                    TransformIssue(
                        "warning",
                        f"No legacy mapping found for DISTRICT value '{raw_district_value}'",
                    ),
                )
        except Exception as e:
            return _result(
                None,
                TransformIssue(
                    "warning",
                    f"Could not derive region from district value '{raw_district_value}': {e}",
                ),
            )

    registry._fns[name] = inner
    return name


def occurrence_from_pop_id_factory(legacy_system: str = "TPFL"):
    """
    Factory that returns a registered transform name for mapping POP_ID to Occurrence ID.

    This transform uses an in-memory cache (populated on first call) that maps
    POP_ID (Occurrence.migrated_from_id) to Occurrence.pk for fast lookups without
    repeated DB queries. The cache persists for the entire migration run.

    Usage:
      OCCURRENCE_FROM_POP_ID = occurrence_from_pop_id_factory("TPFL")
      PIPELINES["Occurrence__migrated_from_id"] = [OCCURRENCE_FROM_POP_ID]

    The transform takes a SHEETNO (read from the row's migrated_from_id field),
    converts it to POP_ID via the legacy mapping, then returns the corresponding
    Occurrence instance.
    """
    key = f"occurrence_from_pop_id_{legacy_system}"
    name = "occurrence_from_pop_id_" + hashlib.sha1(key.encode()).hexdigest()[:8]

    if name in registry._fns:
        return name

    # Create a cache dict that persists for the migration run
    # Maps: POP_ID (string) -> Occurrence PK (int)
    pop_id_to_occurrence_pk_cache = {}
    cache_initialized = [False]  # Use list to allow modification in nested function

    def inner(value, ctx: TransformContext):
        # The input value should be ignored; we read SHEETNO from the row context
        if not ctx or not isinstance(ctx.row, dict):
            return _result(None)

        try:
            from boranga.components.occurrence.models import Occurrence

            # Initialize cache on first call: preload all Occurrences
            if not cache_initialized[0]:
                for occ in Occurrence.objects.filter(migrated_from_id__isnull=False).values("pk", "migrated_from_id"):
                    pop_id_to_occurrence_pk_cache[str(occ["migrated_from_id"])] = occ["pk"]
                cache_initialized[0] = True
                logger.debug(
                    "occurrence_from_pop_id: initialized cache with %d entries",
                    len(pop_id_to_occurrence_pk_cache),
                )

            # Get SHEETNO (migrated_from_id) from the row
            sheetno = ctx.row.get("migrated_from_id")
            if sheetno in (None, ""):
                return _result(None)

            # Strip prefix if present (e.g. "tpfl-12345" -> "12345") because mapping uses raw SHEETNO
            sheetno_str = str(sheetno)
            prefix = f"{legacy_system.lower()}-"
            if sheetno_str.startswith(prefix):
                sheetno_str = sheetno_str[len(prefix) :]

            # Get POP_ID from SHEETNO using the mapping
            pop_id = dm_mappings.get_pop_id_for_sheetno(sheetno_str, legacy_system=legacy_system)
            if not pop_id:
                return _result(None)

            pop_id_str = str(pop_id).strip()

            # Check cache for the occurrence PK
            occ_pk = pop_id_to_occurrence_pk_cache.get(pop_id_str)
            if not occ_pk and legacy_system:
                occ_pk = pop_id_to_occurrence_pk_cache.get(f"{legacy_system.lower()}-{pop_id_str}")

            if occ_pk:
                # Return the Occurrence instance for normalize_create_kwargs to handle
                occurrence = Occurrence.objects.get(pk=occ_pk)
                return _result(occurrence)
            else:
                return _result(None)

        except Exception as e:
            logger.exception("Error in occurrence_from_pop_id_factory: %s", e)
            return _result(None)

    registry._fns[name] = inner
    return name


def occurrence_number_from_pop_id_factory(legacy_system: str = "TPFL"):
    """
    Factory that returns a registered transform name for mapping POP_ID to occurrence_number.

    This transform uses an in-memory cache (populated on first call) that maps
    POP_ID (Occurrence.migrated_from_id) to Occurrence.occurrence_number for fast
    lookups without repeated DB queries. The cache persists for the entire migration run.

    Usage:
      OCCURRENCE_NUMBER_FROM_POP_ID = occurrence_number_from_pop_id_factory("TPFL")
      PIPELINES["ocr_for_occ_number"] = [OCCURRENCE_NUMBER_FROM_POP_ID]

    The transform takes a SHEETNO (read from the row's migrated_from_id field),
    converts it to POP_ID via the legacy mapping, then returns the corresponding
    Occurrence's occurrence_number.
    """
    key = f"occurrence_number_from_pop_id_{legacy_system}"
    name = "occurrence_number_from_pop_id_" + hashlib.sha1(key.encode()).hexdigest()[:8]

    if name in registry._fns:
        return name

    # Create a cache dict that persists for the migration run
    # Maps: POP_ID (string) -> occurrence_number (string)
    pop_id_to_occurrence_number_cache = {}
    cache_initialized = [False]  # Use list to allow modification in nested function

    def inner(value, ctx: TransformContext):
        # The input value should be ignored; we read SHEETNO from the row context
        if not ctx or not isinstance(ctx.row, dict):
            return _result(None)

        try:
            from boranga.components.occurrence.models import Occurrence

            # Initialize cache on first call: preload all Occurrences
            if not cache_initialized[0]:
                for occ in Occurrence.objects.filter(
                    migrated_from_id__isnull=False, occurrence_number__isnull=False
                ).values("occurrence_number", "migrated_from_id"):
                    pop_id_to_occurrence_number_cache[str(occ["migrated_from_id"])] = occ["occurrence_number"]
                cache_initialized[0] = True
                logger.debug(
                    "occurrence_number_from_pop_id: initialized cache with %d entries",
                    len(pop_id_to_occurrence_number_cache),
                )

            # Get SHEETNO (migrated_from_id) from the row
            sheetno = ctx.row.get("migrated_from_id")
            if sheetno in (None, ""):
                return _result(None)

            # Strip prefix if present (e.g. "tpfl-12345" -> "12345") because mapping uses raw SHEETNO
            sheetno_str = str(sheetno)
            prefix = f"{legacy_system.lower()}-"
            if sheetno_str.startswith(prefix):
                sheetno_str = sheetno_str[len(prefix) :]

            # Get POP_ID from SHEETNO using the mapping
            pop_id = dm_mappings.get_pop_id_for_sheetno(sheetno_str, legacy_system=legacy_system)
            if not pop_id:
                return _result(None)

            pop_id_str = str(pop_id).strip()

            # Check cache for the occurrence_number
            occurrence_number = pop_id_to_occurrence_number_cache.get(pop_id_str)
            if not occurrence_number and legacy_system:
                occurrence_number = pop_id_to_occurrence_number_cache.get(f"{legacy_system.lower()}-{pop_id_str}")

            if occurrence_number:
                return _result(occurrence_number)
            else:
                return _result(None)

        except Exception as e:
            logger.exception("Error in occurrence_number_from_pop_id_factory: %s", e)
            return _result(None)

    registry._fns[name] = inner
    return name


def pop_id_from_sheetno_factory(legacy_system: str = "TPFL"):
    """
    Factory that returns a registered transform name for mapping SHEETNO to POP_ID directly.

    This transform converts SHEETNO to POP_ID via the legacy mapping, returning the
    POP_ID string itself (not the occurrence_number).

    Usage:
      POP_ID_FROM_SHEETNO = pop_id_from_sheetno_factory("TPFL")
      PIPELINES["ocr_for_occ_number"] = [POP_ID_FROM_SHEETNO]

    The transform takes a SHEETNO (read from the row's migrated_from_id field),
    converts it to POP_ID via the legacy mapping, then returns the POP_ID string.
    """
    key = f"pop_id_from_sheetno_{legacy_system}"
    name = "pop_id_from_sheetno_" + hashlib.sha1(key.encode()).hexdigest()[:8]

    if name in registry._fns:
        return name

    def inner(value, ctx: TransformContext):
        # The input value should be ignored; we read SHEETNO from the row context
        if not ctx or not isinstance(ctx.row, dict):
            return _result(None)

        try:
            # Get SHEETNO (migrated_from_id) from the row
            sheetno = ctx.row.get("migrated_from_id")
            if sheetno in (None, ""):
                return _result(None)

            # Strip prefix if present (e.g. "tpfl-12345" -> "12345") because mapping uses raw SHEETNO
            sheetno_str = str(sheetno)
            prefix = f"{legacy_system.lower()}-"
            if sheetno_str.startswith(prefix):
                sheetno_str = sheetno_str[len(prefix) :]

            # Get POP_ID from SHEETNO using the mapping
            pop_id = dm_mappings.get_pop_id_for_sheetno(sheetno_str, legacy_system=legacy_system)
            if not pop_id:
                return _result(None)

            # Return POP_ID as string
            return _result(str(pop_id).strip())

        except Exception as e:
            logger.exception("Error in pop_id_from_sheetno_factory: %s", e)
            return _result(None)

    registry._fns[name] = inner
    return name


def geometry_from_coords_factory(
    latitude_field: str = "GDA94LAT",
    longitude_field: str = "GDA94LONG",
    datum_field: str | None = None,
    radius_m: float = 1.0,
):
    """
    Factory that returns a registered transform name for creating WGS84 geometries from lat/lon coordinates.

    This transform reads latitude and longitude from the row context and creates a small buffered
    polygon (default 1m radius) around the point. It handles coordinate transformation from the
    source datum (GDA94, AGD84, WGS84, GPS, etc.) to WGS84 (EPSG:4326).

    Usage:
      GEOMETRY_FROM_COORDS = geometry_from_coords_factory("GDA94LAT", "GDA94LONG", "DATUM", radius_m=1.0)
      PIPELINES["OccurrenceReportGeometry__geometry"] = [GEOMETRY_FROM_COORDS]

    Args:
        latitude_field: The row column name containing latitude (default: GDA94LAT)
        longitude_field: The row column name containing longitude (default: GDA94LONG)
        datum_field: The row column name containing datum/CRS info (optional, default: None)
        radius_m: Buffer radius in meters (default: 1.0m)

    Returns:
        A registered transform name that converts point coordinates to WGS84 polygon geometry
    """
    from django.contrib.gis.geos import Point
    from pyproj import Transformer

    # Map legacy datum values to EPSG codes
    DATUM_TO_EPSG = {
        "GDA94": "EPSG:4283",  # Geocentric Datum of Australia 1994 (modern standard)
        "AGD84": "EPSG:4202",  # Australian Geodetic Datum 1984 (older, superseded)
        "WGS84": "EPSG:4326",  # World Geodetic System 1984
        "GPS": "EPSG:4326",  # GPS typically uses WGS84
        "UNKNOWN": "EPSG:4326",  # Default to WGS84 when datum is unknown
    }

    key = f"geometry_from_coords_{latitude_field}_{longitude_field}_{datum_field}_{radius_m}"
    name = "geometry_from_coords_" + hashlib.sha1(key.encode()).hexdigest()[:8]

    if name in registry._fns:
        return name

    def inner(value, ctx: TransformContext):
        if not ctx or not isinstance(ctx.row, dict):
            return _result(None)

        try:
            lat_str = ctx.row.get(latitude_field)
            lng_str = ctx.row.get(longitude_field)

            if not lat_str or not lng_str:
                return _result(None)

            lat = float(lat_str)
            lng = float(lng_str)

            # Determine source CRS from datum field or default to WGS84
            source_epsg = "EPSG:4326"  # default
            if datum_field:
                datum_str = ctx.row.get(datum_field, "").strip().upper()
                source_epsg = DATUM_TO_EPSG.get(datum_str, "EPSG:4326")

            # Transform coordinates to WGS84 if necessary
            if source_epsg != "EPSG:4326":
                try:
                    transformer = Transformer.from_crs(source_epsg, "EPSG:4326", always_xy=True)
                    lng, lat = transformer.transform(lng, lat)
                except Exception as e:
                    logger.warning(
                        "Failed to transform coordinates from %s to EPSG:4326: %s. Using as-is.",
                        source_epsg,
                        e,
                    )

            # Create a point from the coordinates
            point = Point(lng, lat, srid=4326)

            # Create a small buffer. Convert radius in meters to degrees (approximate):
            # 1 degree ≈ 111,320 meters at the equator
            radius_degrees = radius_m / 111320.0
            buffered = point.buffer(radius_degrees)

            # Ensure SRID is set
            buffered.srid = 4326
            return _result(buffered)

        except Exception as e:
            logger.exception("Error in geometry_from_coords_factory: %s", e)
            return _result(None)

    registry._fns[name] = inner
    return name


@registry.register("community_id_from_legacy")
def t_community_id_from_legacy(value, ctx):
    """
    Transform a legacy community ID (migrated_from_id) into the internal PK
    of the corresponding Community object.
    """
    if not value:
        return _result(None)

    val_str = str(value).strip()
    if not val_str:
        return _result(None)

    mapping = dm_mappings.get_community_id_map()
    pk = mapping.get(val_str)

    if pk is None and ctx and getattr(ctx, "row", None):
        source = ctx.row.get("_source")
        if source:
            prefix = source.lower().replace("_", "-")
            prefixed = f"{prefix}-{val_str}"
            pk = mapping.get(prefixed)

    if pk is None:
        return _result(
            None,
            TransformIssue("warning", f"Community with migrated_from_id '{val_str}' not found"),
        )

    return _result(pk)


# Module-level cache for pyproj transformers
_TRANSFORMER_CACHE = {}


def wkt_to_geometry_factory(source_srid: int = 4326, target_srid: int = 4326) -> str:
    """
    Factory that returns a registered transform name for parsing a WKT string into a GEOSGeometry.

    The returned transform takes a WKT string (from value) then:
    1. Parses it as a Shapely geometry
    2. Uses a cached pyproj.Transformer to reproject if source_srid != target_srid
    3. Converts to Django GEOSGeometry with the target SRID

    Args:
        source_srid: The Spatial Reference ID of the input WKT (default: 4326 for WGS84)
        target_srid: The Spatial Reference ID of the output Geometry (default: 4326 for WGS84)

    Returns:
        A registered transform name (str).
    """
    key = f"wkt_to_geometry_{source_srid}_{target_srid}"
    name = "wkt_to_geom_" + hashlib.sha1(key.encode()).hexdigest()[:8]

    if name in registry._fns:
        return name

    # Load transformer into cache if projection is needed
    if source_srid != target_srid:
        if (source_srid, target_srid) not in _TRANSFORMER_CACHE:
            _TRANSFORMER_CACHE[(source_srid, target_srid)] = Transformer.from_crs(
                f"EPSG:{source_srid}", f"EPSG:{target_srid}", always_xy=True
            )

    def inner(value, ctx: TransformContext):
        """
        Parses WKT string 'value' into GEOSGeometry and performs CRS transformation using pyproj.
        """
        if not value:
            return _result(None)

        try:
            wkt_str = str(value).strip()
            if not wkt_str or wkt_str.lower() == "none" or wkt_str.lower() == "null":
                return _result(None)

            if source_srid == target_srid:
                # No transform needed, direct load
                geom = GEOSGeometry(wkt_str, srid=target_srid)
                return _result(geom)

            # Perform transform using cached pyproj transformer
            transformer = _TRANSFORMER_CACHE[(source_srid, target_srid)]

            # 1. Load WKT into Shapely
            shapely_geom = shapely.wkt.loads(wkt_str)

            # 2. Transform using pyproj
            transformed_shapely = shapely_transform(transformer.transform, shapely_geom)

            # 3. Convert back to GEOSGeometry
            # Use WKB for robust conversion or WKT
            geom = GEOSGeometry(transformed_shapely.wkt, srid=target_srid)

            return _result(geom)

        except Exception as e:
            return _result(
                None,
                TransformIssue("error", f"WKT parse/transform error for value '{value}': {e}"),
            )

    registry._fns[name] = inner
    return name
