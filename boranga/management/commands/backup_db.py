import gzip
import os
import shutil
import subprocess
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    """Management command to create a gzipped custom-format database dump.

    The dump files are written to `.db-backups/<dbname>_<timestamp>.dump.gz` and
    are created with `pg_dump -F c -b` (custom format with blobs), which is
    recommended for PostGIS-enabled databases.

    Example restore (recommended):

    - Streamed restore into an existing database named `mydb`:

        ```bash
        export PGPASSWORD='your_db_password'
        gunzip -c .db-backups/<dbname>_<timestamp>.dump.gz | pg_restore -d mydb --no-owner --no-privileges
        ```

    - Alternatively, uncompress then use `pg_restore` directly:

        ```bash
        gunzip .db-backups/<dbname>_<timestamp>.dump.gz
        pg_restore -d mydb --no-owner --no-privileges .db-backups/<dbname>_<timestamp>.dump
        ```

    Notes:
    - `PGPASSWORD` or `~/.pgpass` may be used to provide credentials to `pg_restore`.
    - The `--no-owner --no-privileges` flags are useful when restoring to
        a different cluster or user to avoid ownership/privilege conflicts.

    """

    help = (
        "Create a gzipped SQL dump (or custom dump) of the configured Django "
        "database in .db-backups (supports PostGIS)"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            dest="database",
            default="default",
            help="Database setting alias to back up (default: 'default')",
        )

    def handle(self, *args, **options):
        db_alias = options.get("database") or "default"
        try:
            db = settings.DATABASES[db_alias]
        except Exception:
            raise CommandError(f"Database settings for '{db_alias}' not found")

        engine = db.get("ENGINE", "")
        # Accept both the standard PostgreSQL and the GeoDjango/PostGIS backends
        if not any(x in engine for x in ("postgres", "postgis")):
            raise CommandError(
                "This command currently supports PostgreSQL/PostGIS only (postgresql/postgis backend)"
            )

        name = db.get("NAME")
        user = db.get("USER")
        password = db.get("PASSWORD")
        host = db.get("HOST") or "localhost"
        port = db.get("PORT") or "5432"

        base_dir = getattr(settings, "BASE_DIR", os.getcwd())
        backups_dir = os.path.join(base_dir, ".db-backups")
        os.makedirs(backups_dir, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        prefix = name or "database"
        # Use a .dump.gz suffix because we'll use pg_dump's custom format (-F c)
        filename = f"{prefix}_{timestamp}.dump.gz"
        path = os.path.join(backups_dir, filename)

        pg_dump_path = shutil.which("pg_dump")
        if not pg_dump_path:
            raise CommandError(
                "pg_dump not found in PATH. Please install PostgreSQL client tools."
            )

        self.stdout.write(
            self.style.NOTICE(f"Starting dump of database '{name}' to '{path}'...")
        )

        env = os.environ.copy()
        if password:
            env["PGPASSWORD"] = password

        # Build pg_dump command. Use custom format (-F c) which handles PostGIS objects
        # and include blobs (-b). Only include -U if a user is provided.
        cmd = [pg_dump_path, "-h", host, "-p", str(port), "-F", "c", "-b", name]
        if user:
            # insert -U <user> before the db name
            cmd[5:5] = ["-U", user]

        # Stream pg_dump stdout into a gzip file to avoid shell usage and large memory use
        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
            )
        except Exception as e:
            raise CommandError(f"Failed to start pg_dump: {e}")

        try:
            with gzip.open(path, "wb") as gz:
                # read in chunks
                while True:
                    chunk = proc.stdout.read(65536)
                    if not chunk:
                        break
                    gz.write(chunk)

            stderr = proc.stderr.read()
            returncode = proc.wait()
            if returncode != 0:
                # remove incomplete file
                try:
                    os.remove(path)
                except Exception:
                    pass
                err_text = (
                    stderr.decode(errors="ignore")
                    if isinstance(stderr, (bytes, bytearray))
                    else str(stderr)
                )
                raise CommandError(f"pg_dump failed (exit {returncode}): {err_text}")

        except Exception as e:
            raise CommandError(f"Error while creating backup: {e}")

        self.stdout.write(self.style.SUCCESS(f"Backup completed: {path}"))
