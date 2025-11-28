import csv

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.functions import Lower
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
            "--verbose",
            action="store_true",
            help="Print per-row result (create/update/skip) and reason",
        )
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

        verbose = options.get("verbose", False)

        # Preload existing mappings for this legacy_system to avoid per-row queries.
        existing_qs = LegacyUsernameEmailuserMapping.objects.filter(
            legacy_system=legacy_system
        )
        existing_map = {m.legacy_username: m for m in existing_qs}

        # Collect emails from CSV to resolve EmailUserRO in bulk (case-insensitive).
        csv_emails = set()
        for r in rows:
            em = (r.get("email") or "").strip()
            if em:
                csv_emails.add(em.lower())

        email_map = {}
        if csv_emails and EmailUserRO is not None:
            try:
                # annotate with lowercase email for case-insensitive matching
                qs = EmailUserRO.objects.annotate(email_l=Lower("email")).filter(
                    email_l__in=list(csv_emails)
                )
                for u in qs:
                    email_map[getattr(u, "email_l")] = u.pk
            except Exception as exc:
                self.stderr.write(f"Error preloading EmailUserROs: {exc}")

        # Classify rows, then apply bulk writes
        to_create = []
        to_update = []
        # Track per-row decisions for verbose output and counts
        decisions = []  # tuples (idx, legacy_username, action, reason)

        for idx, r in enumerate(rows, start=1):
            legacy_username = (
                r.get("legacy_username") or r.get("username") or r.get("legacy") or ""
            ).strip()
            if not legacy_username:
                self.stderr.write(f"Skipping row {idx} with no legacy_username")
                skipped += 1
                decisions.append((idx, None, "skip", "no legacy_username"))
                continue

            first_name = (r.get("first_name") or "").strip()
            last_name = (r.get("last_name") or "").strip()
            email = (r.get("email") or "").strip() or None

            # resolve emailuser_id by email if possible (preloaded map)
            emailuser_id = None
            if email:
                em_l = email.lower()
                if EmailUserRO is None:
                    self.stderr.write(
                        f"Email provided ('{email}') but EmailUserRO model not found; leaving emailuser_id null"
                    )
                else:
                    if em_l in email_map:
                        emailuser_id = email_map[em_l]
                    else:
                        # no match found in preload
                        self.stderr.write(
                            f"No EmailUserRO found for email '{email}'; leaving emailuser_id null"
                        )

            if dry_run:
                # For dry run, just classify and report without modifying DB
                if legacy_username in existing_map:
                    if not do_update:
                        skipped += 1
                        decisions.append(
                            (idx, legacy_username, "skip", "exists (no --update)")
                        )
                        if verbose:
                            self.stdout.write(
                                f"SKIP[{idx}] {legacy_username}: mapping exists (no --update)"
                            )
                    else:
                        existing = existing_map[legacy_username]
                        changed_fields = []
                        if existing.first_name != first_name:
                            changed_fields.append("first_name")
                        if existing.last_name != last_name:
                            changed_fields.append("last_name")
                        if existing.email != email:
                            changed_fields.append("email")
                        if existing.emailuser_id != emailuser_id:
                            changed_fields.append("emailuser_id")
                        if changed_fields:
                            decisions.append(
                                (
                                    idx,
                                    legacy_username,
                                    "update",
                                    ", ".join(changed_fields),
                                )
                            )
                            if verbose:
                                self.stdout.write(
                                    f"UPDATE[{idx}] {legacy_username}: would update fields {', '.join(changed_fields)}"
                                )
                        else:
                            skipped += 1
                            decisions.append(
                                (idx, legacy_username, "skip", "no changes")
                            )
                            if verbose:
                                self.stdout.write(
                                    f"SKIP[{idx}] {legacy_username}: no changes (identical)"
                                )
                else:
                    decisions.append(
                        (
                            idx,
                            legacy_username,
                            "create",
                            f"email={email!r} emailuser_id={emailuser_id}",
                        )
                    )
                    if verbose:
                        self.stdout.write(
                            f"[DRY CREATE][{idx}] {legacy_username}: email={email!r} emailuser_id={emailuser_id}"
                        )
                    # don't actually create in dry run
                continue

            # Non-dry-run: classify into create/update/skip lists, perform batched writes later
            if legacy_username in existing_map:
                existing = existing_map[legacy_username]
                if not do_update:
                    skipped += 1
                    decisions.append(
                        (idx, legacy_username, "skip", "exists (no --update)")
                    )
                    if verbose:
                        self.stdout.write(
                            f"SKIP[{idx}] {legacy_username}: mapping exists (no --update)"
                        )
                    continue
                # check for changes
                changed_fields = []
                if existing.first_name != first_name:
                    existing.first_name = first_name
                    changed_fields.append("first_name")
                if existing.last_name != last_name:
                    existing.last_name = last_name
                    changed_fields.append("last_name")
                if existing.email != email:
                    existing.email = email
                    changed_fields.append("email")
                if existing.emailuser_id != emailuser_id:
                    existing.emailuser_id = emailuser_id
                    changed_fields.append("emailuser_id")
                if changed_fields:
                    to_update.append(existing)
                    decisions.append(
                        (idx, legacy_username, "update", ", ".join(changed_fields))
                    )
                    if verbose:
                        self.stdout.write(
                            f"UPDATE[{idx}] {legacy_username}: queued update fields {', '.join(changed_fields)}"
                        )
                else:
                    skipped += 1
                    decisions.append((idx, legacy_username, "skip", "no changes"))
                    if verbose:
                        self.stdout.write(
                            f"SKIP[{idx}] {legacy_username}: no changes (identical)"
                        )
            else:
                # create new instance (not saved yet)
                inst = LegacyUsernameEmailuserMapping(
                    legacy_system=legacy_system,
                    legacy_username=legacy_username,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    emailuser_id=emailuser_id,
                )
                to_create.append(inst)
                decisions.append(
                    (
                        idx,
                        legacy_username,
                        "create",
                        f"email={email!r} emailuser_id={emailuser_id}",
                    )
                )
                if verbose:
                    self.stdout.write(
                        f"CREATE[{idx}] {legacy_username}: queued create email={email!r} emailuser_id={emailuser_id}"
                    )

        # Apply batched writes
        try:
            with transaction.atomic():
                created = 0
                updated = 0
                if to_create:
                    LegacyUsernameEmailuserMapping.objects.bulk_create(
                        to_create, batch_size=500
                    )
                    created = len(to_create)
                if to_update:
                    LegacyUsernameEmailuserMapping.objects.bulk_update(
                        to_update,
                        ["first_name", "last_name", "email", "emailuser_id"],
                        batch_size=500,
                    )
                    updated = len(to_update)
                # adjust skipped: total rows - created - updated - errors
                processed = created + updated
                skipped = len(rows) - processed - errors
        except Exception as exc:
            self.stderr.write(f"Failed to apply batched writes: {exc}")
            errors += 1

        self.stdout.write(
            f"created={created} updated={updated} skipped={skipped} errors={errors} (dry_run={dry_run})"
        )
