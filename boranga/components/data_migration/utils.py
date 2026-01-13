from __future__ import annotations

from datetime import datetime
from typing import Any


def to_int_maybe(v: Any) -> int | None:
    """Return int(v) or None for None/''/invalid."""
    if v is None or v == "":
        return None
    if isinstance(v, int):
        return v
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def to_bool_maybe(v: Any) -> bool | None:
    """Coerce common truthy/falsey values to bool or None for empty."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "t"):
        return True
    if s in ("0", "false", "no", "n", "f"):
        return False
    return None


def parse_date_iso(v: Any) -> datetime | None:
    """Try to parse ISO-like date/time strings; return None on failure."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.fromisoformat(s) if "T" in s else datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        # last resort: fromisoformat (handles offsets)
        return datetime.fromisoformat(s)
    except Exception:
        return None


def to_decimal_maybe(v: Any) -> Any | None:
    """Return Decimal(v) or None for None/''/invalid."""
    from decimal import Decimal, InvalidOperation

    if v is None or v == "":
        return None
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def safe_strip(v: Any) -> str | None:
    """Trim whitespace and return None for empty strings or None inputs."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def to_float_maybe(v: Any) -> float | None:
    """Return float(v) or None for None/''/invalid."""
    if v is None or v == "":
        return None
    if isinstance(v, (float, int)):
        return float(v)
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
