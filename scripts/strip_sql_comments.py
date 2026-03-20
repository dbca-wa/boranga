#!/usr/bin/env python3
"""
Strip comments from SQL files in docs/sql-scripts/ and print the result.

Usage:
    python scripts/strip_sql_comments.py [file ...]

Without arguments, processes all .sql files in docs/sql-scripts/.
With arguments, processes only the named files (paths or bare filenames).

The original files are never modified.
"""

import re
import sys
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent.parent / "docs" / "sql-scripts"


def strip_sql_comments(sql: str) -> str:
    """Remove -- line comments and /* ... */ block comments from SQL text."""
    # Pattern handles:
    #   - single-quoted strings  'it''s a "test"'  (preserve content)
    #   - double-quoted identifiers  "my col"         (preserve content)
    #   - block comments  /* ... */                   (remove)
    #   - line comments   -- ...                      (remove)
    #
    # Approach: tokenise left-to-right, keeping literals intact.

    result = []
    i = 0
    n = len(sql)

    while i < n:
        # --- Single-quoted string literal ---
        if sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    j += 2  # escaped quote inside string
                elif sql[j] == "'":
                    j += 1
                    break
                else:
                    j += 1
            result.append(sql[i:j])
            i = j

        # --- Double-quoted identifier ---
        elif sql[i] == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"' and j + 1 < n and sql[j + 1] == '"':
                    j += 2
                elif sql[j] == '"':
                    j += 1
                    break
                else:
                    j += 1
            result.append(sql[i:j])
            i = j

        # --- Block comment /* ... */ ---
        elif sql[i : i + 2] == "/*":
            j = sql.find("*/", i + 2)
            if j == -1:
                # Unterminated block comment — drop the rest
                break
            i = j + 2

        # --- Line comment -- ... ---
        elif sql[i : i + 2] == "--":
            j = sql.find("\n", i + 2)
            if j == -1:
                break  # comment runs to EOF
            i = j  # keep the newline so line numbers stay roughly intact

        else:
            result.append(sql[i])
            i += 1

    text = "".join(result)

    # Collapse runs of blank lines (>2 consecutive) left behind by removed comments
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def resolve_files(args: list[str]) -> list[Path]:
    if not args:
        return sorted(SQL_DIR.glob("*.sql"))

    paths = []
    for arg in args:
        p = Path(arg)
        if not p.is_absolute() and not p.exists():
            # Try treating it as a bare filename inside SQL_DIR
            candidate = SQL_DIR / arg
            if candidate.exists():
                p = candidate
        if not p.exists():
            print(f"Warning: file not found: {arg}", file=sys.stderr)
            continue
        paths.append(p.resolve())
    return paths


def main() -> None:
    files = resolve_files(sys.argv[1:])

    if not files:
        print("No SQL files found.", file=sys.stderr)
        sys.exit(1)

    for path in files:
        if len(files) > 1:
            print(f"-- ===== {path.name} =====")
            print()

        sql = path.read_text(encoding="utf-8")
        print(strip_sql_comments(sql))

        if len(files) > 1:
            print()


if __name__ == "__main__":
    main()
