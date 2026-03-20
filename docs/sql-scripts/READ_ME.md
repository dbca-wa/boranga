KB does not support sql comments.

To generate the sql scripts into the kb sub folder with comments removed, simply run:

for f in docs/sql-scripts/*.sql; do python scripts/strip_sql_comments.py "$f" > "docs/sql-scripts/kb/$(basename "$f")"; done