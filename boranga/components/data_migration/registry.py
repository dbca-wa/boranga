from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from datetime import timezone as stdlib_timezone
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Literal

import nh3
from django.core.cache import cache
from django.db import models
from django.utils import timezone
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.data_migration import mappings as dm_mappings
from boranga.components.main.models import (
    LegacyUsernameEmailuserMapping,
    LegacyValueMap,
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
    level: str  # 'error' | 'warning'
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
    # Add more shared info as required


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
            # allow either a registered transform name (str) or a callable transform
            if callable(n):
                pipeline.append(n)
                continue
            try:
                pipeline.append(self._fns[n])
            except KeyError:
                raise KeyError(
                    f"unknown transform {n!r} (pipeline entry must be a registered name or a callable)"
                )
        return pipeline


registry = TransformRegistry()


def _result(value, *issues: TransformIssue):
    return TransformResult(value=value, issues=list(issues))


# ------------------------ Common Transform Functions ------------------------


@registry.register("strip")
def t_strip(value, ctx):
    if value is None:
        return _result(value)
    return _result(str(value).strip())


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


@registry.register("required")
def t_required(value, ctx):
    if value in (None, "", []):
        return _result(value, TransformIssue("error", "Value required"))
    return _result(value)


@registry.register("is_present")
def t_is_present(value, ctx):
    if value in (None, "", []):
        return _result(False)
    return _result(True)


@registry.register("Y_to_active_else_historical")
def t_Y_to_active_else_historical(value, ctx):
    if value == "Y":
        return _result("active")
    return _result("historical")


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


def to_decimal_factory(
    max_digits: int | None = None, decimal_places: int | None = None
):
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
                    TransformIssue(
                        "error", f"Failed to quantize to {decimal_places} dp: {e}"
                    ),
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
    return _result(
        value, TransformIssue("error", f"Unrecognized date format: {value!r}")
    )


@registry.register("datetime_iso")
def t_datetime_iso(value, ctx):
    """
    Parse datetimes (including ISO strings) and return a timezone-aware datetime
    in UTC suitable for Django (USE_TZ=True).

    If the incoming string contains an explicit UTC offset token (+0000 / +00:00 / Z)
    we strip that token and treat the remainder as naive UTC (useful when source
    emits +0000 style offsets that some parsers don't accept).
    """
    if not value:
        return _result(None)

    s = str(value).strip()
    dt = None

    # If the string ends with a timezone offset, capture it.
    m = re.search(r"([+-]\d{2}:?\d{2}|Z)$", s)
    if m:
        tz_token = m.group(1)
        # Treat explicit UTC tokens as no-offset (strip them and parse as UTC)
        if tz_token in ("Z", "+0000", "+00:00", "+0000", "+00:00"):
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
        return _result(
            value, TransformIssue("error", f"Unrecognized datetime format: {value!r}")
        )

    # Make timezone-aware in UTC for Django:
    if dt.tzinfo is None:
        # treat naive datetimes as UTC and make them aware
        dt = timezone.make_aware(dt, stdlib_timezone.utc)
    else:
        # convert any aware datetime to UTC
        dt = dt.astimezone(stdlib_timezone.utc)

    return _result(dt)


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
    if not value:
        return _result(None)

    s = str(value).strip()
    dt = None

    # strip explicit UTC tokens (keep non-UTC offsets intact)
    m = re.search(r"([+-]\d{2}:?\d{2}|Z)$", s)
    if m:
        tz_token = m.group(1)
        if tz_token in ("Z", "+0000", "+00:00"):
            s = s[: m.start(1)].rstrip()

    # try stdlib ISO parsing
    try:
        iso = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(iso)
    except Exception:
        dt = None

    # try dateutil if available
    if dt is None:
        try:
            from dateutil import parser

            dt = parser.parse(s)
        except Exception:
            dt = None

    # fallback legacy formats
    if dt is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                dt = None

    if dt is None:
        return _result(
            value, TransformIssue("error", f"Unrecognized datetime format: {value!r}")
        )

    # normalise to UTC date
    if dt.tzinfo is None:
        dt = timezone.make_aware(dt, stdlib_timezone.utc)
    else:
        dt = dt.astimezone(stdlib_timezone.utc)

    return _result(dt.date())


@registry.register("ocr_comments_transform")
def t_ocr_comments_transform(value, ctx):
    PURPOSE1 = ctx.row.get("PURPOSE1", "").strip()
    PURPOSE2 = ctx.row.get("PURPOSE2", "").strip()
    VESTING = ctx.row.get("VESTING", "").strip()
    FENCING_STATUS = ctx.row.get("FENCING_STATUS", "").strip()
    FENCING_COMMENTS = ctx.row.get("FENCING_COMMENTS", "").strip()
    ROADSIDE_MARKER_STATUS = ctx.row.get("ROADSIDE_MARKER_STATUS", "").strip()
    RDSIDE_MKR_COMMENTS = ctx.row.get("RDSIDE_MKR_COMMENTS", "").strip()

    comments = None
    transform_issues = []

    if PURPOSE1:
        purpose1 = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="PURPOSE (DRF_LOV_PURPOSE_VWS)",
            value=PURPOSE1,
            use_cache=True,
        )
        if not purpose1:
            transform_issues.append(
                TransformIssue(
                    "error", f"No Purpose found that maps to legacy value: {value!r}"
                )
            )

    if PURPOSE2:
        purpose2 = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="PURPOSE (DRF_LOV_PURPOSE_VWS)",
            value=PURPOSE2,
            use_cache=True,
        )
        if not purpose2:
            transform_issues.append(
                TransformIssue(
                    "error", f"No Purpose found that maps to legacy value: {value!r}"
                )
            )

    if VESTING:
        vesting = LegacyValueMap.get_target(
            legacy_system="TPFL",
            list_name="VESTING (DRF_LOV_VESTING_VWS)",
            value=VESTING,
            use_cache=True,
        )
        if not vesting:
            transform_issues.append(
                TransformIssue(
                    "error", f"No Vesting found that maps to legacy value: {value!r}"
                )
            )

    if PURPOSE1:
        comments = purpose1.current_value

    if PURPOSE2:
        if comments:
            comments += ", "
        comments += purpose2.current_value

    if VESTING:
        if comments:
            comments += ", "
        comments += f"Vesting: {vesting.current_value}"

    if FENCING_STATUS:
        if comments:
            comments += ", "
        comments = f"Fencing Status: {FENCING_STATUS}"
    if FENCING_COMMENTS:
        if comments:
            comments += ", "
        comments += f"Fencing Comments: {FENCING_COMMENTS}"
    if ROADSIDE_MARKER_STATUS:
        if comments:
            comments += ", "
        comments += f"Roadside Marker Status: {ROADSIDE_MARKER_STATUS}"
    if RDSIDE_MKR_COMMENTS:
        if comments:
            comments += ", "
        comments += f"Roadside Marker Comments: {RDSIDE_MKR_COMMENTS}"

    return _result(comments, *transform_issues)


def fk_lookup(model: type[models.Model], lookup_field: str = "id"):
    key = f"fk_{model._meta.label_lower}_{lookup_field}"

    @registry.register(key)
    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)
        qs = model._default_manager

        # Try lookup using the stored (cleaned) form first, then the raw value.
        try:
            cleaned = nh3.clean(str(value))
        except Exception:
            cleaned = None

        candidates = []
        if cleaned and cleaned != str(value):
            candidates.append(cleaned)
        candidates.append(value)

        for candidate in candidates:
            try:
                obj = qs.get(**{lookup_field: candidate})
                return _result(obj.pk)
            except model.DoesNotExist:
                continue
            except model.MultipleObjectsReturned:
                return _result(
                    value,
                    TransformIssue(
                        "error",
                        f"Multiple {model.__name__} with {lookup_field}='{value}' found",
                    ),
                )

        return _result(
            value,
            TransformIssue(
                "error",
                f"{model.__name__} with {lookup_field}='{value}' not found",
            ),
        )

    return key


def taxonomy_lookup(lookup_field: str = "scientific_name", check_previous: bool = True):
    """
    Resolve a Taxonomy by lookup_field (e.g. 'scientific_name' or 'taxon_name_id').
    Behaviour:
      - tries cleaned (nh3.clean) then raw value against Taxonomy.{lookup_field}
      - if not found and check_previous=True, looks for a TaxonPreviousName.previous_scientific_name
        matching the value (case-insensitive) and returns that previous_name.taxonomy.pk
    Returns a registered transform name.
    """
    key = f"taxonomy_lookup_{lookup_field}_{int(check_previous)}"

    @registry.register(key)
    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)

        # Prepare candidates: try cleaned first, then raw
        try:
            cleaned = nh3.clean(str(value))
        except Exception:
            cleaned = None

        candidates = []
        # Database values will be stored in cleaned form, so prefer only the cleaned candidates.
        # Also try variants: cleaned with trailing " PN" removed, and cleaned with a trailing
        # varietal suffix like " var. ..." removed.
        if cleaned:
            candidates.append(cleaned)
            # candidate with trailing " PN" removed
            if isinstance(cleaned, str) and cleaned.endswith(" PN"):
                candidates.append(cleaned[:-3].rstrip())
            # TODO: Remove commented code once confirmed not needed
            # candidate with " var. ..." (case-insensitive) stripped
            # if isinstance(cleaned, str):
            #     var_stripped = re.sub(r"\s*var\..*$", "", cleaned, flags=re.IGNORECASE).rstrip()
            #     if var_stripped and var_stripped != cleaned:
            #         candidates.append(var_stripped)
        else:
            # fallback to raw value if cleaning failed
            candidates.append(value)

        qs = Taxonomy._default_manager

        # Try direct Taxonomy lookup
        for candidate in candidates:
            try:
                obj = qs.get(**{lookup_field: candidate})
                return _result(obj.pk)
            except Taxonomy.DoesNotExist:
                continue
            except Taxonomy.MultipleObjectsReturned:
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
                # prefer case-insensitive match for textual previous names
                if isinstance(candidate, str):
                    prev_qs = TaxonPreviousName.objects.filter(
                        previous_scientific_name__iexact=str(candidate).strip()
                    )
                else:
                    # numeric id fallback (previous_name_id)
                    prev_qs = TaxonPreviousName.objects.filter(
                        previous_name_id=candidate
                    )

                if not prev_qs.exists():
                    continue

                if prev_qs.count() > 1:
                    return _result(
                        value,
                        TransformIssue(
                            "error",
                            f"Multiple TaxonPreviousName entries for '{value}' found",
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
            TransformIssue(
                "error", f"Taxonomy with {lookup_field}='{value}' not found"
            ),
        )

    return key


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
        return TransformResult(
            value=None, issues=[TransformIssue("error", "group_type required")]
        )
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
        return _result(
            value, TransformIssue("error", f"User with email='{value}' not found")
        )
    except EmailUserRO.MultipleObjectsReturned:
        return _result(
            value, TransformIssue("error", f"Multiple users with email='{value}'")
        )


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

    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)
        username = str(value).strip()
        qs = LegacyUsernameEmailuserMapping.objects.filter(
            legacy_system__iexact=str(legacy_system), legacy_username__iexact=username
        )
        try:
            mapping = qs.get()
            return _result(mapping.emailuser_id)
        except LegacyUsernameEmailuserMapping.DoesNotExist:
            return _result(
                value,
                TransformIssue(
                    "error",
                    f"User with legacy username='{value}' not found for system='{legacy_system}'",
                ),
            )
        except LegacyUsernameEmailuserMapping.MultipleObjectsReturned:
            return _result(
                value,
                TransformIssue(
                    "error",
                    f"Multiple users with legacy username='{value}' for system='{legacy_system}'",
                ),
            )

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
        if not isinstance(value, (list, tuple)):
            return _result(
                value, TransformIssue("error", "Expected list for multiselect")
            )
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
            "csv_lookup_map:"
            + hashlib.sha1(
                f"{resolved_path}:{case_insensitive}:{delimiter}".encode()
            ).hexdigest()
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


def run_pipeline(
    pipeline: list[TransformFn], value: Any, ctx: TransformContext
) -> TransformResult:
    current = TransformResult(value)
    for fn in pipeline:
        step_res = fn(current.value, ctx)
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
        raise KeyError(
            f"Importer slug '{slug}' not found. Available: {', '.join(sorted(_importer_registry))}"
        )


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
        dep = (
            ctx.row.get(dependency_column)
            if ctx and isinstance(ctx.row, dict)
            else None
        )

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
                    return _result(
                        value, TransformIssue("error", f"Unknown transform '{mapping}'")
                    )
                try:
                    res = fn(dep, ctx)
                    if isinstance(res, TransformResult):
                        return res
                    return _result(res)
                except Exception as e:
                    return _result(
                        value, TransformIssue("error", f"mapping transform error: {e}")
                    )

            # callable mapping
            if callable(mapping):
                try:
                    res = mapping(dep, ctx)
                    if isinstance(res, TransformResult):
                        return res
                    return _result(res)
                except Exception as e:
                    return _result(
                        value, TransformIssue("error", f"mapping callable error: {e}")
                    )

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
                    TransformIssue(
                        "error", f"Unknown value for {dependency_column}: {dep!r}"
                    ),
                )
            if warning_on_unknown:
                return _result(
                    value,
                    TransformIssue(
                        "warning", f"Unknown value for {dependency_column}: {dep!r}"
                    ),
                )
            return _result(default)

        # no mapping or mapper -> return the dependency value (or default if absent)
        return _result(dep if dep is not None else default)

    registry._fns[name] = _inner
    return name


def dependent_apply_transform_factory(
    dependency_column: str, transform_name: str
) -> str:
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
        dep = (
            ctx.row.get(dependency_column)
            if ctx and isinstance(ctx.row, dict)
            else None
        )

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
            return _result(
                default, TransformIssue("error", f"pluck attribute error: {e}")
            )

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
                issues=[
                    TransformIssue(
                        "error", f"Unmapped {legacy_system}.{list_name} value '{value}'"
                    )
                ],
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
                        TransformIssue(
                            "error", f"Unrecognized date for formatting: {value!r}"
                        ),
                    )
            else:
                return _result(
                    value,
                    TransformIssue(
                        "error", f"No parser available for date value: {value!r}"
                    ),
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
                return _result(
                    value, TransformIssue("error", f"expected prefix {pref!r}")
                )
            return _result(None)
        m = re.search(r"(" + r"\d+" + r")\s*$", s)
        if not m:
            if required:
                return _result(
                    value,
                    TransformIssue(
                        "error", f"no trailing digits after prefix {pref!r}"
                    ),
                )
            return _result(None)
        try:
            return _result(int(m.group(1)))
        except Exception as e:
            return _result(
                value, TransformIssue("error", f"to_int_trailing failed: {e}")
            )

    return _transform
