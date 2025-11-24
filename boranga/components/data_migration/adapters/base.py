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
            # Determine optional limit (priority: explicit option, then env var)
            limit = None
            try:
                if options is not None:
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

            # For Excel files we rely on pandas' read_excel (no streaming support here).
            if ext in (".xls", ".xlsx"):
                df = pd.read_excel(path, dtype=str)
                df = df.fillna("")
                rows = df.to_dict(orient="records")
                if limit and isinstance(rows, list):
                    rows = rows[: int(limit)]
                return rows, warnings

            # For CSV-like files: if a limit is provided, stream only enough rows
            # instead of loading the entire file into memory.
            if limit and limit > 0:
                try:
                    # Use chunksize equal to the requested limit and read only first chunk
                    it = pd.read_csv(
                        path, dtype=str, encoding=encoding, chunksize=int(limit)
                    )
                    try:
                        df = next(it)
                    except StopIteration:
                        df = pd.DataFrame()
                    df = df.fillna("")
                    rows = df.to_dict(orient="records")
                    # If the chunk produced more rows than limit (unlikely), slice
                    if len(rows) > int(limit):
                        rows = rows[: int(limit)]
                    return rows, warnings
                except Exception:
                    # Fall back to full read if chunked read fails for any reason
                    pass

            # Default (no limit): read whole CSV into memory as before
            df = pd.read_csv(path, dtype=str, encoding=encoding)
            df = df.fillna("")
            rows = df.to_dict(orient="records")
        except Exception as e:
            warnings.append(ExtractionWarning(f"Failed reading file with pandas: {e}"))
        return rows, warnings
