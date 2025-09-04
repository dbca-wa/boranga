from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from datetime import timezone as stdlib_timezone
from typing import Any

from django.db import models
from django.utils import timezone
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.main.models import LegacyUsernameEmailuserMapping
from boranga.components.species_and_communities.models import GroupType

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

    def build_pipeline(self, names: Sequence[str]) -> list[TransformFn]:
        return [self._fns[n] for n in names]


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
        dt = stdlib_timezone.utc.make_aware(dt, stdlib_timezone.utc)
    else:
        dt = dt.astimezone(stdlib_timezone.utc)

    return _result(dt.date())


def fk_lookup(model: type[models.Model], lookup_field: str = "id", create=False):
    key = f"fk_{model._meta.label_lower}_{lookup_field}_{int(create)}"

    @registry.register(key)
    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)
        qs = model._default_manager
        try:
            # logger.debug(f"Lookup field: {lookup_field}, value: {value}")
            obj = qs.get(**{lookup_field: value})
        except model.DoesNotExist:
            if create:
                obj = model._default_manager.create(**{lookup_field: value})
            else:
                return _result(
                    value,
                    TransformIssue(
                        "error",
                        f"{model.__name__} with {lookup_field}='{value}' not found",
                    ),
                )
        return _result(obj.pk)

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
