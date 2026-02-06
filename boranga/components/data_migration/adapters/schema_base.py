from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from boranga.components.data_migration.registry import choices_transform

NormalizeFn = Callable[[str], str]


@dataclass
class ChildExpansionSpec:
    name: str  # e.g. 'habitats'
    index_pattern: str | None = None  # e.g. r"habitat_(\d+)_(type|area)"
    fields: dict | None = None  # optional explicit mapping
    split_columns: dict | None = None  # e.g. {"tags": {"sep": ";", "field": "tag"}}


@dataclass
class Schema:
    column_map: dict[str, str]
    required: list[str]
    pipelines: dict[str, list[str]]
    source_choices: list[str] | None = None
    normalize_header_fn: NormalizeFn = lambda h: h.strip()
    # internal cache
    _inverse: dict[str, str] = field(default_factory=dict, init=False)
    _choice_transform_name: str | None = None
    child_specs: list[ChildExpansionSpec] | None = None

    def __post_init__(self):
        self._inverse = {v: k for k, v in self.column_map.items()}
        if self.source_choices:
            # register / get transform name once
            self._choice_transform_name = choices_transform(self.source_choices)

    # ------------- Header / column helpers -------------
    def normalise_header(self, h: str) -> str:
        return self.normalize_header_fn(h)

    def canonical_key(self, raw_header: str) -> str | None:
        return self.column_map.get(self.normalise_header(raw_header))

    def required_missing(self, canonical_present: Iterable[str]) -> list[str]:
        present = set(canonical_present)
        return [c for c in self.required if c not in present]

    def validate_headers(self, raw_headers: Iterable[str]) -> tuple[list[str], list[str]]:
        norm = [self.normalise_header(h) for h in raw_headers]
        canonical_present = [self.column_map[h] for h in norm if h in self.column_map]
        errors: list[str] = []
        warnings: list[str] = []
        missing = self.required_missing(canonical_present)
        if missing:
            errors.append(f"Missing required columns: {', '.join(missing)}")
        unknown = [h for h in norm if h not in self.column_map]
        if unknown:
            warnings.append(f"Unrecognized columns: {', '.join(unknown)}")
        return errors, warnings

    def map_raw_row(self, raw: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for hdr, val in raw.items():
            key = self.canonical_key(hdr)
            if key:
                out[key] = val
        return out

    # ------------- Pipeline helpers -------------
    def effective_pipelines(self) -> dict[str, list[str]]:
        """
        Returns pipelines with dynamic choice transform substituted if requested.
        If a column uses placeholder token "{SOURCE_CHOICE}" replace it.
        """
        if not self._choice_transform_name:
            return self.pipelines
        resolved: dict[str, list[str]] = {}
        for col, seq in self.pipelines.items():
            resolved[col] = [(self._choice_transform_name if step == "{SOURCE_CHOICE}" else step) for step in seq]
        return resolved
