import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExtractionWarning:
    message: str
    row_hint: Any | None = None


@dataclass
class ExtractionResult:
    rows: list[dict[str, Any]]
    warnings: list[ExtractionWarning] = field(default_factory=list)


class SourceAdapter:
    source_key: str  # must match sources.Source value

    # Optional: domain name for logging
    domain: str | None = None

    def extract(self, path: str, **options) -> ExtractionResult:
        raise NotImplementedError

    # Optionally a quick probe (if you want auto-detect)
    def can_auto_detect(self) -> bool:
        # Only true if subclass overrides detect
        return self.__class__.detect is not SourceAdapter.detect

    def detect(self, path: str, **options) -> bool:
        """Return True if this adapter applies (implement only if needed)."""
        raise NotImplementedError

    def read_table(
        self, path: str, *, encoding: str = "utf-8", **options
    ) -> tuple[list[dict], list["ExtractionWarning"]]:
        """
        Read a flat table file into a list[dict] using pandas (assumed available).
        Returns (rows, warnings).
        """
        rows: list[dict] = []
        warnings: list[ExtractionWarning] = []
        try:
            import pandas as pd

            ext = os.path.splitext(path)[1].lower()
            if ext in (".xls", ".xlsx"):
                df = pd.read_excel(path, dtype=str)
            else:
                df = pd.read_csv(path, dtype=str, encoding=encoding)
            df = df.fillna("")
            rows = df.to_dict(orient="records")
            # Apply optional row-limit for faster testing. Priority:
            # 1. explicit `limit` in options
            # 2. DATA_MIGRATION_LIMIT env var
            limit = None
            if options is not None:
                try:
                    limit_option = options.get("limit")
                    if limit_option is not None:
                        limit = int(limit_option)
                except Exception:
                    limit = None
            if limit is None:
                try:
                    env = os.environ.get("DATA_MIGRATION_LIMIT")
                    if env:
                        limit = int(env)
                except Exception:
                    limit = None
            if limit and isinstance(rows, list):
                rows = rows[: int(limit)]
        except Exception as e:
            warnings.append(ExtractionWarning(f"Failed reading file with pandas: {e}"))
        return rows, warnings
