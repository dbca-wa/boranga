from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import time
from urllib.parse import urlparse

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from boranga.components.spatial.models import Proxy


class Command(BaseCommand):
    help = "Import a WFS layer into PostGIS using ogr2ogr (reads DB creds from Django settings)."

    def add_arguments(self, parser):
        # wfs_url may be omitted; when omitted a Proxy record's proxy_url will be used
        # as the geoserver root and --layer must be supplied
        parser.add_argument(
            "wfs_url",
            nargs="?",
            help="Full WFS GetFeature URL (include typeName and outputFormat)",
        )
        parser.add_argument(
            "--table",
            "-t",
            help="Destination table name (schema.table or table)",
            required=True,
        )
        parser.add_argument("--nln", help="OGR -nln (if you need override)", default=None)
        parser.add_argument(
            "--layer",
            help="workspace:layer_name (used to construct WFS URL from Proxy.proxy_url when wfs_url omitted)",
        )
        parser.add_argument("--srid", help="Target SRID (e.g. 4326)", type=int, default=4326)
        parser.add_argument("--overwrite", action="store_true", help="Pass -overwrite to ogr2ogr")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--extra-args", help="Extra args to pass to ogr2ogr (quoted)", default="")
        parser.add_argument(
            "--page-count",
            type=int,
            default=2000,
            help="Page size for WFS paging (use small values if server times out)",
        )
        parser.add_argument(
            "--use-geojson-paging",
            action="store_true",
            help="Download the layer in GeoJSON pages and import each page (safer for large layers)",
        )
        parser.add_argument(
            "--proxy-request-path",
            help="Use Proxy with this request_path to set HTTP(S)_PROXY and credentials (lookup in Proxy model)",
            default=None,
        )

    def handle(self, *args, **options):
        # honour the CLI flag immediately to avoid any full-download/probe path
        use_geojson_paging = bool(options.get("use_geojson_paging", False))
        if use_geojson_paging:
            self.stdout.write(
                "use_geojson_paging=True — will perform paged downloads and skip single-request fetch/probe"
            )
        else:
            self.stdout.write("use_geojson_paging=False")

        db = settings.DATABASES["default"]
        user = db.get("USER") or ""
        name = db.get("NAME")
        password = db.get("PASSWORD") or ""
        host = db.get("HOST") or "localhost"
        port = db.get("PORT") or ""
        if not name:
            raise CommandError("DATABASES['default']['NAME'] not set")
        # build PG connection string for ogr2ogr
        pg_conn = f"PG:host={host} user={user} dbname={name} password={password} port={port}"
        wfs_url = options.get("wfs_url")
        layer = options.get("layer")
        proxy_request_path = options.get("proxy_request_path")

        # If no wfs_url provided, require a Proxy (proxy_request_path) and layer
        proxy = None
        if not wfs_url:
            if not proxy_request_path:
                raise CommandError("Either wfs_url or --proxy-request-path (and --layer) must be provided")
            try:
                proxy = Proxy.objects.get(request_path=proxy_request_path, active=True)
            except Proxy.DoesNotExist:
                raise CommandError(f"No active Proxy found for request_path={proxy_request_path!r}")
            if not layer:
                raise CommandError(
                    "--layer is required when wfs_url is omitted; it will be combined with Proxy.proxy_url"
                )
            # construct WFS GetFeature URL from proxy.proxy_url (assumed root geoserver URL)
            base = proxy.proxy_url.rstrip("/")
            wfs_url = (
                f"{base}/ows?service=WFS&version=2.0.0&request=GetFeature&typeName="
                f"{layer}&outputFormat=application/json"
            )
        else:
            # wfs_url provided; optionally fetch proxy for HTTP proxy/env if requested
            if proxy_request_path:
                try:
                    proxy = Proxy.objects.get(request_path=proxy_request_path, active=True)
                except Proxy.DoesNotExist:
                    raise CommandError(f"No active Proxy found for request_path={proxy_request_path!r}")

        dest_table = options["table"]
        nln = options["nln"] or dest_table
        srid = options["srid"]
        overwrite = options["overwrite"]
        extra = options["extra_args"] or ""

        # If we have a Proxy row and basic auth is enabled, prefer fetching the WFS
        # with requests (authenticated) and pass a local temp file to ogr2ogr.
        fetched_tempfile = None
        if proxy and getattr(proxy, "basic_auth_enabled", False):
            auth = (proxy.username, proxy.password)
            self.stdout.write("Fetching WFS as authenticated request to avoid embedding creds in URL...")
            try:
                with requests.get(wfs_url, auth=auth, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    tmp = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False)
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            tmp.write(chunk)
                    tmp.flush()
                    tmp.close()
                    fetched_tempfile = tmp.name
                    # replace the WFS source for ogr2ogr with the local file
                    wfs_source = fetched_tempfile
            except Exception as exc:
                raise CommandError(f"Failed to fetch WFS as authenticated request: {exc}")
        else:
            wfs_source = wfs_url

        # build args_list using wfs_source instead of wfs_url (base args used below per-page)
        args_list = [
            "ogr2ogr",
            "-f",
            "PostgreSQL",
            pg_conn,
            wfs_source,
            "-nln",
            nln,
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-lco",
            "GEOMETRY_NAME=geom",
            "-lco",
            "SPATIAL_INDEX=YES",
            "-a_srs",
            f"EPSG:{srid}",
            "-t_srs",
            f"EPSG:{srid}",
        ]
        if overwrite:
            args_list.append("-overwrite")
        if extra:
            args_list += shlex.split(extra)

        self.stdout.write("Running ogr2ogr with:")
        self.stdout.write(shlex.join(args_list))
        if options["dry_run"]:
            return

        # prepare environment for subprocess
        run_env = os.environ.copy()

        # paging import (GeoJSON) - safer for large layers / server flow-control
        page_count = options.get("page_count", 2000)
        # ensure we keep the early-decided value (avoid accidental overwrite later)
        use_geojson_paging = use_geojson_paging
        if use_geojson_paging:
            # use session (with auth if proxy provided)
            session = requests.Session()
            if proxy and getattr(proxy, "basic_auth_enabled", False):
                session.auth = (proxy.username, proxy.password)

            # ensure we have cookies (kick capabilities) - tolerate failure
            try:
                caps_url = urlparse(wfs_url)
                caps_base = f"{caps_url.scheme}://{caps_url.netloc}{caps_url.path}"
                # Request capabilities to establish GS_FLOW_CONTROL cookie if needed
                session.get(
                    f"{caps_base}?service=WFS&version=2.0.0&request=GetCapabilities",
                    timeout=(10, 30),
                )
            except Exception:
                pass

            start = 0
            first = True
            while True:
                page_url = f"{wfs_url}&count={page_count}&startIndex={start}&outputFormat=application/json"
                self.stdout.write(f"Fetching page startIndex={start} count={page_count}")
                try:
                    with session.get(page_url, stream=True, timeout=(10, 300)) as r:
                        r.raise_for_status()
                        tmp = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False)
                        for chunk in r.iter_content(chunk_size=64 * 1024):
                            if chunk:
                                tmp.write(chunk)
                        tmp.flush()
                        tmp.close()
                        fname = tmp.name
                except Exception as exc:
                    raise CommandError(f"Failed to fetch page startIndex={start}: {exc}")

                # quick inspect page to decide whether to continue
                try:
                    with open(fname, encoding="utf-8") as fh:
                        doc = json.load(fh)
                        num = doc.get("numberReturned") or len(doc.get("features", []))
                except Exception:
                    # not valid json -> likely an error HTML from server
                    head = open(fname, "rb").read(1024)
                    raise CommandError(f"Page {start} is not valid GeoJSON; head={head[:400]!r}")

                if num == 0:
                    os.unlink(fname)
                    self.stdout.write("No features in page -> finished")
                    break

                # build ogr2ogr args for this page
                ogr_args = [
                    "ogr2ogr",
                    "-f",
                    "PostgreSQL",
                    pg_conn,
                    fname,
                    "-nln",
                    nln,
                    "-nlt",
                    "PROMOTE_TO_MULTI",
                    "-lco",
                    "GEOMETRY_NAME=geom",
                    "-a_srs",
                    f"EPSG:{srid}",
                    "-t_srs",
                    f"EPSG:{srid}",
                ]
                if first and overwrite:
                    ogr_args.append("-overwrite")
                elif not first:
                    ogr_args.append("-append")
                if extra:
                    ogr_args += shlex.split(extra)

                self.stdout.write("Running ogr2ogr on page: " + shlex.join(ogr_args))
                try:
                    subprocess.run(ogr_args, check=True, env=run_env)
                except subprocess.CalledProcessError as e:
                    raise CommandError(f"ogr2ogr failed on page startIndex={start}: {e}")
                finally:
                    try:
                        os.unlink(fname)
                    except Exception:
                        pass

                first = False
                start += page_count
                time.sleep(1)

            # close session and finish
            session.close()
            self.stdout.write(self.style.SUCCESS(f"Imported WFS layer to {nln} (paged)"))
            return

        try:
            subprocess.run(args_list, check=True, env=run_env)
        finally:
            # cleanup temp file if we created one
            if fetched_tempfile:
                try:
                    os.unlink(fetched_tempfile)
                except Exception:
                    pass

        self.stdout.write(self.style.SUCCESS(f"Imported WFS layer to {nln}"))
