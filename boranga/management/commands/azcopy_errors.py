import logging
import os
import re
import subprocess
import tempfile
import zipfile

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Zips the latest error reports from private-media/handler_output and copies them via azcopy"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Prepare the zip file but do not execute azcopy command",
        )

    def handle(self, *args, **options):
        # 1. Check Environment
        azcopy_dest = os.environ.get("AZCOPY_PATH")
        if not azcopy_dest:
            raise CommandError("Environment variable AZCOPY_PATH is not set.")

        # 2. Locate Directory
        # Assuming private-media is at the project root based on settings.BASE_DIR usually
        # But commonly it is defined relative to the specific deployment structure.
        # Based on file listing, it is /data/data/projects/boranga/private-media
        # settings.BASE_DIR in boranga/settings.py is dirname(dirname(abspath(__file__))) which is /data/data/projects/boranga

        base_dir = settings.BASE_DIR
        handler_output_dir = os.path.join(base_dir, "private-media", "handler_output")

        if not os.path.exists(handler_output_dir):
            raise CommandError(f"Directory not found: {handler_output_dir}")

        self.stdout.write(f"Scanning directory: {handler_output_dir}")

        # 3. Find Latest Files
        files_map = {}  # slug -> (timestamp, filename)

        # Regex to match {slug}_errors_{YYYYMMDD_HHMMSS}.csv
        # We look for the last occurrence of _errors_ just in case, or stick to the strict suffix pattern
        pattern = re.compile(r"^(.*)_errors_(\d{8}_\d{6})\.csv$")

        for fname in os.listdir(handler_output_dir):
            match = pattern.match(fname)
            if match:
                slug = match.group(1)
                timestamp = match.group(2)

                if slug not in files_map:
                    files_map[slug] = (timestamp, fname)
                else:
                    # Compare timestamps (lexicographical comparison works for YYYYMMDD_HHMMSS)
                    current_max_ts, _ = files_map[slug]
                    if timestamp > current_max_ts:
                        files_map[slug] = (timestamp, fname)
            else:
                # debug log if needed for unmatched files?
                pass

        if not files_map:
            self.stdout.write(self.style.WARNING("No error files found matching pattern."))
            return

        files_to_zip = [info[1] for info in files_map.values()]
        self.stdout.write(self.style.SUCCESS(f"Found {len(files_to_zip)} latest error files:"))
        for f in sorted(files_to_zip):
            self.stdout.write(f" - {f}")

        # 4. Create Zip File
        # We can create a temporary zip file
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
            zip_filename = tmp_zip.name

        try:
            self.stdout.write(f"Creating zip file: {zip_filename}")
            with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
                for fname in files_to_zip:
                    full_path = os.path.join(handler_output_dir, fname)
                    zf.write(full_path, arcname=fname)

            # 5. Run AzCopy
            if options["dry_run"]:
                self.stdout.write(
                    self.style.SUCCESS(f'[Dry Run] Would execute: azcopy copy "{zip_filename}" "{azcopy_dest}"')
                )
            else:
                self.stdout.write(f"Executing azcopy copy to {azcopy_dest}...")

                cmd = ["azcopy", "copy", zip_filename, azcopy_dest]

                # First attempt
                result = subprocess.run(cmd)

                # If failed, it might be auth. Try to login and retry.
                if result.returncode != 0:
                    self.stdout.write(
                        self.style.WARNING(
                            "AzCopy failed. Assuming authentication required. Initiating azcopy login..."
                        )
                    )

                    # Run login interactively
                    login_result = subprocess.run(["azcopy", "login"])

                    if login_result.returncode == 0:
                        self.stdout.write(self.style.SUCCESS("Login successful. Retrying copy..."))
                        result = subprocess.run(cmd)
                    else:
                        raise CommandError("AzCopy login failed.")

                if result.returncode == 0:
                    self.stdout.write(self.style.SUCCESS("AzCopy completed successfully."))
                else:
                    raise CommandError(f"AzCopy failed with exit code {result.returncode}")

        finally:
            # Cleanup temp file
            if os.path.exists(zip_filename):
                try:
                    os.remove(zip_filename)
                    # self.stdout.write("Cleaned up temporary zip file.")
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"Failed to remove temp file {zip_filename}: {e}"))
