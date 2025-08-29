from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from django.db import models
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.species_and_communities.models import GroupType

TransformFn = Callable[[Any, "TransformContext"], "TransformResult"]


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


def fk_lookup(model: type[models.Model], lookup_field: str = "id", create=False):
    key = f"fk_{model._meta.label_lower}_{lookup_field}_{int(create)}"

    @registry.register(key)
    def inner(value, ctx):
        if value in (None, ""):
            return _result(None)
        qs = model._default_manager
        try:
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
