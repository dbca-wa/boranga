# Clean, single-definition implementation below.
import json
import os
import shutil
import subprocess
import time

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from requests.auth import HTTPBasicAuth

from .import_cadastre_helpers import build_pg_connection as _build_pg_connection
from .import_cadastre_helpers import find_geometry_column as _find_geometry_column
from .import_cadastre_helpers import load_meta as _load_meta
from .import_cadastre_helpers import meta_dir as _meta_dir
from .import_cadastre_helpers import meta_path as _meta_path
from .import_cadastre_helpers import save_meta as _save_meta


class Command(BaseCommand):
    help = (
        "Download the KB cadastre GeoJSON layer (KB_CADASTRE_LAYER_URL) and import into PostGIS using ogr2ogr. "
        "Supports streaming chunking (ijson) to reduce peak memory and a --chunks-only dry-run."
    )

    def add_arguments(self, parser):
        parser.add_argument("--schema", default="public", help="Target DB schema (default: public)")
        parser.add_argument(
            "--table",
            help=("Target table name. If omitted, defaults to the KB_LAYER_TABLE setting or 'kb_cadastre'."),
        )
        parser.add_argument(
            "--srid",
            type=int,
            default=4326,
            help="Target SRID for the layer (default: 4326)",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Overwrite the target table if it exists (uses ogr2ogr -overwrite)",
        )
        parser.add_argument(
            "--no-index",
            action="store_true",
            help="Do not create a spatial index after import",
        )
        parser.add_argument(
            "--keep-temp",
            action="store_true",
            help=("Do not delete the downloaded temp file after import " "(keeps file under .kb_layer_cache)."),
        )
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=20000,
            help=(
                "Split the downloaded GeoJSON into chunks of this many features and "
                "import sequentially. Streams features using the Python ijson parser "
                "(no external JSON tooling required). Useful to reduce ogr2ogr peak memory. "
                "Default: 20000. Set to 0 to disable chunking."
            ),
        )
        parser.add_argument(
            "--use-cached",
            action="store_true",
            help=(
                "If present and a previously downloaded temp file exists under "
                ".kb_layer_cache, reuse that file instead of downloading again. "
                "Useful for faster local testing (requires --keep-temp was used previously)."
            ),
        )
        parser.add_argument(
            "--chunks-only",
            action="store_true",
            help=(
                "Only split the GeoJSON into chunk files and do not run ogr2ogr. "
                "Chunk files are written under .kb_layer_cache/chunks_<ts>/. Use "
                "this to validate chunking without importing."
            ),
        )
        parser.add_argument(
            "--skip-if-unchanged",
            action="store_true",
            help=(
                "If present, perform a conditional HEAD against the remote resource "
                "using saved ETag/Last-Modified and skip the download/import when the "
                "server reports 304 Not Modified."
            ),
        )
        parser.add_argument(
            "--check-auth",
            action="store_true",
            help=(
                "Verify that provided authentication "
                "(KB_AUTH_USER/KB_AUTH_PASS or KB_AUTH_HEADER) can access the remote "
                "URL and exit. Useful in CI or deploy scripts to validate credentials "
                "without downloading the full layer."
            ),
        )

    def _meta_dir(self):
        return _meta_dir()

    def _meta_path(self, table):
        return _meta_path(table)

    def _load_meta(self, table):
        return _load_meta(table)

    def _save_meta(self, table, headers):
        return _save_meta(table, headers)

    def _build_pg_connection(self):
        try:
            conn, password = _build_pg_connection()
            return conn, password
        except Exception as e:
            raise CommandError(str(e))

    def _run_ogr2ogr(self, src, dst_pg, layer_name, srid, overwrite, env, cachemax=64):
        cmd = [
            "ogr2ogr",
            "--config",
            "PG_USE_COPY",
            "YES",
            "--config",
            "GDAL_CACHEMAX",
            str(cachemax),
        ]
        if overwrite:
            cmd.append("-overwrite")
        cmd.extend(
            [
                "-f",
                "PostgreSQL",
                dst_pg,
                src,
                "-nln",
                layer_name,
                "-nlt",
                "PROMOTE_TO_MULTI",
                "-t_srs",
                f"EPSG:{srid}",
                "-lco",
                "GEOMETRY_NAME=geom",
                "-lco",
                "FID=gid",
                "-lco",
                "PRECISION=NO",
            ]
        )

        if not shutil.which("ogr2ogr"):
            raise CommandError("ogr2ogr not found on PATH. Install GDAL (ogr2ogr) to use this command.")

        # Run ogr2ogr and print output to console; do not write logs to disk.
        self.stdout.write("Running: {}".format(" ".join(cmd)))
        try:
            proc = subprocess.run(cmd, check=True, env=env, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            err = e.stderr or e.stdout or str(e)
            raise CommandError(f"ogr2ogr failed: {err}")

        if proc.stdout:
            self.stdout.write(proc.stdout)
        if proc.stderr:
            self.stderr.write(proc.stderr)

    def _import_chunks(
        self,
        src_file,
        dst_pg,
        layer_name,
        srid,
        overwrite,
        env,
        chunk_size,
        do_import=True,
        keep_chunks=False,
    ):
        import decimal
        import json

        import ijson

        base_dir = self._meta_dir()
        work_dir = os.path.join(base_dir, f"chunks_{int(time.time())}")
        os.makedirs(work_dir, exist_ok=True)

        chunk_files = []
        current = 0
        chunk_index = 0
        outfh = None
        try:
            with open(src_file, "rb") as srcf:
                features = ijson.items(srcf, "features.item")

                def _json_default(o):
                    try:
                        if isinstance(o, decimal.Decimal):
                            return float(o)
                    except Exception:
                        pass
                    return str(o)

                first_in_chunk = True
                for feat in features:
                    if current % int(chunk_size) == 0:
                        if outfh:
                            outfh.write("]}")
                            outfh.close()
                        partname = os.path.join(work_dir, f"part_{chunk_index:05d}.geojson")
                        outfh = open(partname, "w", encoding="utf-8")
                        outfh.write('{"type":"FeatureCollection","features":[')
                        chunk_files.append(partname)
                        chunk_index += 1
                        first_in_chunk = True
                    if not first_in_chunk:
                        outfh.write(",")
                    else:
                        first_in_chunk = False
                    outfh.write(json.dumps(feat, ensure_ascii=False, default=_json_default))
                    current += 1
                if outfh:
                    outfh.write("]}")
                    outfh.close()
        except Exception as e:
            try:
                if outfh and not outfh.closed:
                    outfh.close()
            except Exception:
                pass
            raise CommandError(f"Failed to stream features for chunking: {e}")

        if do_import:
            imported_any = False
            for i, chunk in enumerate(chunk_files):
                self.stdout.write(f"Importing chunk {i+1}/{len(chunk_files)}: {os.path.basename(chunk)}")
                if not imported_any:
                    self._run_ogr2ogr(chunk, dst_pg, layer_name, srid, True, env)
                    imported_any = True
                else:
                    cmd = [
                        "ogr2ogr",
                        "--config",
                        "PG_USE_COPY",
                        "YES",
                        "--config",
                        "GDAL_CACHEMAX",
                        "256",
                        "-append",
                        "-f",
                        "PostgreSQL",
                        dst_pg,
                        chunk,
                        "-nln",
                        layer_name,
                        "-nlt",
                        "PROMOTE_TO_MULTI",
                        "-t_srs",
                        f"EPSG:{srid}",
                        "-lco",
                        "GEOMETRY_NAME=geom",
                        "-lco",
                        "FID=gid",
                        "-lco",
                        "PRECISION=NO",
                    ]
                    try:
                        subprocess.run(cmd, check=True, env=env, capture_output=True, text=True)
                    except subprocess.CalledProcessError as e:
                        raise CommandError(f"ogr2ogr append failed for chunk {chunk}: {e.stderr or e.stdout or str(e)}")

        # Remove chunk files unless caller requested they be kept (e.g., --chunks-only + --keep-temp)
        if keep_chunks:
            self.stdout.write(self.style.NOTICE(f"Keeping chunk directory: {work_dir}"))
        else:
            try:
                for f in os.listdir(work_dir):
                    os.unlink(os.path.join(work_dir, f))
                os.rmdir(work_dir)
            except Exception:
                pass

    def _find_geometry_column(self, schema, table):
        return _find_geometry_column(connection, schema, table)

    def handle(self, *args, **options):
        schema = options["schema"]
        table = options.get("table")
        srid = options["srid"]
        overwrite = options["overwrite"]
        no_index = options["no_index"]

        url = getattr(settings, "KB_CADASTRE_LAYER_URL", None)
        if not url:
            raise CommandError("KB_CADASTRE_LAYER_URL not configured in settings")

        user = getattr(settings, "KB_AUTH_USER", None)
        pwd = getattr(settings, "KB_AUTH_PASS", None)
        auth_header = getattr(settings, "KB_AUTH_HEADER", None)
        table_from_settings = getattr(settings, "KB_LAYER_TABLE", None)

        if not table and table_from_settings:
            table = table_from_settings
        if not table:
            table = "kb_cadastre"

        saved = self._load_meta(table) or {}
        remote_headers = None
        skip_if_unchanged = options.get("skip_if_unchanged")
        # --check-auth: perform a lightweight auth/access probe and exit.
        if options.get("check_auth"):
            probe_kwargs = {"allow_redirects": True, "timeout": 30}
            if user and pwd:
                probe_kwargs["auth"] = HTTPBasicAuth(user, pwd)
            elif auth_header:
                headers = {}
                if ":" in auth_header:
                    k, v = auth_header.split(":", 1)
                    headers[k.strip()] = v.strip()
                else:
                    headers["Authorization"] = auth_header.strip()
                probe_kwargs["headers"] = headers

            try:
                hr = requests.head(url, **probe_kwargs)
            except Exception:
                # Fallback to a short streamed GET to verify access without downloading the body
                try:
                    get_kwargs = {"stream": True, "timeout": 10}
                    if user and pwd:
                        get_kwargs["auth"] = HTTPBasicAuth(user, pwd)
                    if probe_kwargs.get("headers"):
                        get_kwargs["headers"] = probe_kwargs["headers"]
                    gr = requests.get(url, **get_kwargs)
                    gr.close()
                    hr = gr
                except Exception as e:
                    raise CommandError(f"Auth check failed: {e}")

            if hr is not None and hr.status_code < 400:
                self.stdout.write(self.style.SUCCESS(f"Auth check succeeded (HTTP {hr.status_code})"))
                return
            raise CommandError(f"Auth check failed (HTTP {hr.status_code if hr is not None else 'error'})")

        if skip_if_unchanged:
            head_kwargs = {"allow_redirects": True, "timeout": 30}
            if user and pwd:
                head_kwargs["auth"] = HTTPBasicAuth(user, pwd)
            elif auth_header:
                headers = {}
                if ":" in auth_header:
                    k, v = auth_header.split(":", 1)
                    headers[k.strip()] = v.strip()
                else:
                    headers["Authorization"] = auth_header.strip()
                head_kwargs["headers"] = headers

            cond_headers = {}
            if saved.get("etag"):
                cond_headers["If-None-Match"] = saved.get("etag")
            if saved.get("last_modified"):
                cond_headers["If-Modified-Since"] = saved.get("last_modified")
            if cond_headers:
                head_kwargs.setdefault("headers", {}).update(cond_headers)

            try:
                hr = requests.head(url, **head_kwargs)
            except Exception:
                try:
                    get_kwargs = {"stream": True, "timeout": 30}
                    if user and pwd:
                        get_kwargs["auth"] = HTTPBasicAuth(user, pwd)
                    if head_kwargs.get("headers"):
                        get_kwargs["headers"] = head_kwargs["headers"]
                    gr = requests.get(url, **get_kwargs)
                    gr.close()
                    hr = gr
                except Exception:
                    hr = None

            if hr is not None and hr.status_code == 304:
                self.stdout.write(self.style.NOTICE("Server returned 304 Not Modified; skipping import."))
                return
            if hr is not None and hr.status_code < 400:
                remote_headers = hr.headers
                remote_etag = hr.headers.get("ETag") or hr.headers.get("etag")
                remote_lm = hr.headers.get("Last-Modified") or hr.headers.get("last-modified")

                def _norm(v):
                    if not v:
                        return None
                    s = v.strip()
                    if s.startswith("W/"):
                        s = s[2:]
                    if s.startswith('"') and s.endswith('"') and len(s) >= 2:
                        s = s[1:-1]
                    return s

                remote_etag = _norm(remote_etag)
                remote_lm = _norm(remote_lm)
                if remote_etag and saved.get("etag") and remote_etag == saved.get("etag"):
                    self.stdout.write(self.style.NOTICE("Remote ETag matches saved ETag; skipping import."))
                    return
                if remote_lm and saved.get("last_modified") and remote_lm == saved.get("last_modified"):
                    self.stdout.write(self.style.NOTICE("Remote Last-Modified matches saved value; skipping import."))
                    return

        dst_pg, password = self._build_pg_connection()
        env = os.environ.copy()
        if password:
            env["PGPASSWORD"] = str(password)

        if user and pwd:
            env["GDAL_HTTP_USERPWD"] = f"{user}:{pwd}"
            env["GDAL_HTTP_USERNAME"] = str(user)
            env["GDAL_HTTP_PASSWORD"] = str(pwd)
            env["GDAL_HTTP_AUTH"] = "Basic"

        self.stdout.write(self.style.NOTICE("Using streaming download to a temp file."))
        meta_dir = self._meta_dir()
        ts = int(time.time())
        tmp_name = os.path.join(meta_dir, f"kb_layer_{ts}.geojson")

        use_cached = options.get("use_cached")
        if use_cached:
            candidate = None
            if saved.get("file"):
                candidate = os.path.join(meta_dir, saved.get("file"))
            else:
                globs = [
                    os.path.join(meta_dir, p)
                    for p in os.listdir(meta_dir)
                    if p.startswith("kb_layer_") and p.endswith(".geojson")
                ]
                if globs:
                    candidate = sorted(globs)[-1]
            if candidate and os.path.exists(candidate):
                self.stdout.write(self.style.NOTICE(f"Reusing cached file: {candidate}"))
                tmp_name = candidate

        if not (use_cached and os.path.exists(tmp_name)):
            self.stdout.write(f"Downloading {url} to temporary file {tmp_name} ...")
            start = time.perf_counter()
            req_kwargs = {"stream": True, "timeout": 120}
            if user and pwd:
                req_kwargs["auth"] = HTTPBasicAuth(user, pwd)
                self.stdout.write(self.style.NOTICE("Using HTTP Basic auth for download"))
            elif auth_header:
                headers = {}
                if ":" in auth_header:
                    k, v = auth_header.split(":", 1)
                    headers[k.strip()] = v.strip()
                else:
                    headers["Authorization"] = auth_header.strip()
                req_kwargs["headers"] = headers

            with requests.get(url, **req_kwargs) as r:
                r.raise_for_status()
                remote_headers = r.headers
                with open(tmp_name, "wb") as tmpf:
                    for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                        if chunk:
                            tmpf.write(chunk)
            elapsed = time.perf_counter() - start
            try:
                size = os.path.getsize(tmp_name)
            except Exception:
                size = None
            self.stdout.write(self.style.NOTICE(f"Download finished: {size or 'unknown'} bytes in {elapsed:.2f}s"))

        try:
            import hashlib

            h = hashlib.sha256()
            with open(tmp_name, "rb") as _fh:
                for block in iter(lambda: _fh.read(1024 * 1024), b""):
                    h.update(block)
            sha = h.hexdigest()
            try:
                m = self._load_meta(table) or {}
                m.update({"file": os.path.basename(tmp_name), "sha256": sha})
                with open(self._meta_path(table), "w", encoding="utf-8") as _mfh:
                    json.dump(m, _mfh)
            except Exception:
                pass
        except Exception:
            sha = None

        chunks_only = options.get("chunks_only")
        chunk_size = options.get("chunk_size")
        if chunk_size:
            try:
                keep_chunks = bool(chunks_only and options.get("keep_temp"))
                self._import_chunks(
                    tmp_name,
                    dst_pg,
                    f"{schema}.{table}",
                    srid,
                    overwrite,
                    env,
                    int(chunk_size),
                    do_import=not chunks_only,
                    keep_chunks=keep_chunks,
                )
                self.stdout.write(self.style.SUCCESS(f"Chunks created under {self._meta_dir()}"))
                if chunks_only:
                    if not options.get("keep_temp"):
                        try:
                            os.unlink(tmp_name)
                        except Exception:
                            pass
                    return
            except Exception as e:
                raise CommandError(f"Chunked import failed: {e}")
        else:
            self._run_ogr2ogr(tmp_name, dst_pg, f"{schema}.{table}", srid, overwrite, env)

        if not options.get("keep_temp"):
            try:
                os.unlink(tmp_name)
            except Exception:
                pass

        if not no_index:
            geom_col = self._find_geometry_column(schema, table)
            if not geom_col:
                self.stdout.write(
                    self.style.WARNING("Could not find geometry column to index; skipping index creation")
                )
                return

            idx_name = f"{table}_{geom_col}_gist"
            create_index_sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{table} USING GIST ({geom_col});"
            self.stdout.write(f"Creating spatial index: {idx_name}")
            with connection.cursor() as cursor:
                cursor.execute(create_index_sql)

        try:
            if remote_headers:
                self._save_meta(table, remote_headers)
        except Exception:
            pass

        self.stdout.write(self.style.SUCCESS(f"Imported {url} into {schema}.{table}"))
