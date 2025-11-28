import csv

from django.core.management.base import BaseCommand
from django.db import transaction
from ledger_api_client.ledger_models import EmailUserRO

from boranga.components.main.models import LegacyUsernameEmailuserMapping


class Command(BaseCommand):
    help = (
        "Import LegacyUsernameEmailuserMapping rows from CSV. "
        "Columns: legacy_username (required), first_name, last_name, email. "
        "Provide legacy system via --legacy-system. emailuser_id will be resolved "
        "by looking up EmailUserRO by email."
    )

    def add_arguments(self, parser):
        parser.add_argument("csvfile", type=str)
        parser.add_argument("--legacy-system", required=True)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--update", action="store_true", help="update existing rows when present"
        )

    def handle(self, *args, **options):
        csvfile = options["csvfile"]
        legacy_system = options["legacy_system"]
        dry_run = options["dry_run"]
        do_update = options["update"]

        rows = []
        # Use 'utf-8-sig' to handle BOMs and skip initial spaces after delimiters.
        with open(csvfile, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh, skipinitialspace=True)
            # Normalize header fieldnames to strip whitespace and any BOM marker.
            if reader.fieldnames:
                reader.fieldnames = [
                    (fn.strip().lstrip("\ufeff") if fn is not None else fn)
                    for fn in reader.fieldnames
                ]
            for r in reader:
                # Ensure keys are normalized per-row too (defensive).
                normalized_row = {}
                for k, v in r.items():
                    if k is None:
                        continue
                    nk = k.strip().lstrip("\ufeff")
                    normalized_row[nk] = v
                rows.append(normalized_row)

        created = 0
        updated = 0
        skipped = 0
        errors = 0

        for r in rows:
            legacy_username = (
                r.get("legacy_username") or r.get("username") or r.get("legacy") or ""
            ).strip()
            if not legacy_username:
                self.stderr.write("Skipping row with no legacy_username")
                skipped += 1
                continue

            first_name = (r.get("first_name") or "").strip()
            last_name = (r.get("last_name") or "").strip()
            email = (r.get("email") or "").strip() or None

            # resolve emailuser_id by email if possible
            emailuser_id = None
            if email:
                if EmailUserRO is None:
                    self.stderr.write(
                        f"Email provided ('{email}') but EmailUserRO model not found; leaving emailuser_id null"
                    )
                else:
                    try:
                        candidates = list(
                            EmailUserRO.objects.filter(email__iexact=email)[:2]
                        )
                        if len(candidates) == 1:
                            emailuser_id = candidates[0].pk
                        elif len(candidates) > 1:
                            emailuser_id = candidates[0].pk
                            self.stderr.write(
                                f"Multiple EmailUserRO matches for email '{email}'; using id {emailuser_id}"
                            )
                        else:
                            self.stderr.write(
                                f"No EmailUserRO found for email '{email}'; leaving emailuser_id null"
                            )
                    except Exception as exc:
                        self.stderr.write(
                            f"Error querying EmailUserRO for email '{email}': {exc}"
                        )
                        errors += 1
                        continue

            if dry_run:
                self.stdout.write(
                    f"[DRY] would create/update: legacy_system={legacy_system} legacy_username={legacy_username} "
                    f"first_name={first_name!r} last_name={last_name!r} email={email!r} emailuser_id={emailuser_id}"
                )
                continue

            try:
                with transaction.atomic():
                    existing = LegacyUsernameEmailuserMapping.objects.filter(
                        legacy_system=legacy_system, legacy_username=legacy_username
                    ).first()
                    if existing:
                        if not do_update:
                            skipped += 1
                            continue
                        # update existing row if values differ
                        changed = False
                        if existing.first_name != first_name:
                            existing.first_name = first_name
                            changed = True
                        if existing.last_name != last_name:
                            existing.last_name = last_name
                            changed = True
                        if existing.email != email:
                            existing.email = email
                            changed = True
                        if existing.emailuser_id != emailuser_id:
                            existing.emailuser_id = emailuser_id
                            changed = True
                        if changed:
                            existing.save()
                            updated += 1
                        else:
                            skipped += 1
                    else:
                        LegacyUsernameEmailuserMapping.objects.create(
                            legacy_system=legacy_system,
                            legacy_username=legacy_username,
                            first_name=first_name,
                            last_name=last_name,
                            email=email,
                            emailuser_id=emailuser_id,
                        )
                        created += 1
            except Exception as exc:
                self.stderr.write(
                    f"Failed to process username '{legacy_username}': {exc}"
                )
                errors += 1

        self.stdout.write(
            f"created={created} updated={updated} skipped={skipped} errors={errors} (dry_run={dry_run})"
        )
