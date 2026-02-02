import json
import os
from urllib.parse import urlparse

from django.conf import settings


def meta_dir():
    base = getattr(settings, "BASE_DIR", None) or os.getcwd()
    d = os.path.join(base, ".kb_layer_cache")
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return d


def meta_path(table):
    safe = table.replace(".", "_")
    return os.path.join(meta_dir(), f"{safe}.meta.json")


def load_meta(table):
    path = meta_path(table)
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_meta(table, headers):
    path = meta_path(table)
    meta = {}
    etag = headers.get("ETag") or headers.get("etag")
    lm = headers.get("Last-Modified") or headers.get("last-modified")

    def _norm_etag(v):
        if not v:
            return None
        s = v.strip()
        if s.startswith("W/"):
            s = s[2:]
        if s.startswith('"') and s.endswith('"') and len(s) >= 2:
            s = s[1:-1]
        return s

    if etag:
        meta["etag"] = _norm_etag(etag)
    if lm:
        meta["last_modified"] = lm.strip()

    try:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as _old:
                try:
                    old = json.load(_old) or {}
                except Exception:
                    old = {}
            for k in ("file", "sha256"):
                if old.get(k) and not meta.get(k):
                    meta[k] = old.get(k)
    except Exception:
        pass

    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(meta, fh)
    except Exception:
        pass


def build_pg_connection():
    db = settings.DATABASES.get("default")
    if not db:
        raise RuntimeError("DATABASES['default'] not configured in settings")

    def _quote(val):
        if val is None:
            return None
        s = str(val)
        if not any(c.isspace() for c in s) and "'" not in s and '"' not in s and "\\" not in s:
            return s
        s = s.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{s}'"

    raw_db_url = db.get("URL") or os.environ.get("DATABASE_URL")
    user = None
    password = None
    host = None
    port = None
    name = None

    if raw_db_url:
        p = urlparse(raw_db_url)
        if p.scheme and p.scheme.startswith("postgres"):
            user = p.username
            password = p.password
            host = p.hostname
            port = p.port
            name = p.path[1:] if p.path and p.path.startswith("/") else (p.path or None)

    if not name:
        name = db.get("NAME")
    if not user:
        user = db.get("USER") or os.environ.get("USER")
    if not password:
        password = db.get("PASSWORD") or os.environ.get("PGPASSWORD")
    if not host:
        host = db.get("HOST") or ""
    if not port:
        port = db.get("PORT") or ""

    parts = []
    if host:
        parts.append(f"host={_quote(host)}")
    if port:
        parts.append(f"port={_quote(port)}")
    if user:
        parts.append(f"user={_quote(user)}")
    if name:
        parts.append(f"dbname={_quote(name)}")

    conn = "PG:" + " ".join(parts)
    return conn, password


def find_geometry_column(conn, schema, table):
    # conn is a Django connection object
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT f_geometry_column FROM geometry_columns " "WHERE f_table_schema=%s AND f_table_name=%s",
            [schema, table],
        )
        row = cursor.fetchone()
        if row and row[0]:
            return row[0]

        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s AND udt_name='geometry'",
            [schema, table],
        )
        row = cursor.fetchone()
        if row:
            return row[0]

    return None
