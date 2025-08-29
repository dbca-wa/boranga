import csv

from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import transaction

from boranga.components.main.models import LegacyValueMap

APP_LABEL = "boranga"  # all target models live in this app


class Command(BaseCommand):
    help = (
        "Import LegacyValueMap rows from CSV. Columns: list_name, legacy_value, "
        "target_model, target_lookup_field_name, target_lookup_field_value, "
        "optional target_lookup_field_name_2/target_lookup_field_value_2, canonical_name, active"
    )

    def add_arguments(self, parser):
        parser.add_argument("csvfile", type=str)
        parser.add_argument("--legacy-system", required=True)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--update", action="store_true", help="update existing rows"
        )

    def _resolve_content_type(self, model_name: str | None) -> ContentType | None:
        if not model_name:
            return None
        model = model_name.strip().lower()
        try:
            return ContentType.objects.get(app_label=APP_LABEL, model=model)
        except ContentType.DoesNotExist:
            return None

    def handle(self, *args, **options):
        csvfile = options["csvfile"]
        legacy_system = options["legacy_system"]
        dry_run = options["dry_run"]
        do_update = options["update"]

        rows = []
        with open(csvfile, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                rows.append(r)

        created = 0
        updated = 0
        skipped = 0
        with transaction.atomic():
            for r in rows:
                legacy_value = (r.get("legacy_value") or r.get("legacy") or "").strip()
                if not legacy_value:
                    self.stderr.write("Skipping row with no legacy_value")
                    skipped += 1
                    continue

                # list_name is taken per-row from the CSV (required)
                row_list_name = (
                    r.get("list_name") or r.get("list") or ""
                ).strip() or None
                if not row_list_name:
                    self.stderr.write(
                        f"Missing list_name for legacy_value '{legacy_value}'; skipping"
                    )
                    skipped += 1
                    continue

                canonical = (
                    r.get("canonical_name") or r.get("canonical") or ""
                ).strip()
                active = r.get("active", "true").strip().lower() not in (
                    "0",
                    "false",
                    "no",
                )

                # required: model and lookup field/value (we no longer accept target_object_id)
                target_model = (
                    r.get("target_model") or r.get("model") or ""
                ).strip() or None
                # default lookup field to "name" when the CSV cell is empty
                lookup_field = (
                    r.get("target_lookup_field_name") or r.get("lookup_field") or ""
                ).strip()
                if not lookup_field:
                    lookup_field = "name"
                lookup_value = (
                    r.get("target_lookup_field_value") or r.get("lookup_value") or ""
                ).strip() or None

                # optional second lookup pair to further filter the target (parent/related)
                # If provided, this will be added to the queryset filter as an additional key.
                # Accepts related lookups using Django lookup syntax (e.g. "parent__name").
                lookup_field_2 = (
                    r.get("target_lookup_field_name_2") or r.get("lookup_field_2") or ""
                ).strip() or None
                lookup_value_2 = (
                    r.get("target_lookup_field_value_2")
                    or r.get("lookup_value_2")
                    or ""
                ).strip() or None

                if not target_model:
                    self.stderr.write(
                        f"Missing target_model; skipping row '{legacy_value}'"
                    )
                    skipped += 1
                    continue
                if not lookup_value:
                    self.stderr.write(
                        f"Missing lookup_value for model '{target_model}' (lookup_field='{lookup_field}'); "
                        f"skipping row '{legacy_value}'"
                    )
                    skipped += 1
                    continue

                # Resolve lookup -> target_id
                try:
                    model_cls = apps.get_model(APP_LABEL, target_model.strip().lower())
                except (LookupError, ValueError):
                    self.stderr.write(
                        f"Unknown model '{target_model}' in app '{APP_LABEL}'; skipping row '{legacy_value}'"
                    )
                    skipped += 1
                    continue
                try:
                    # assemble filter kwargs; include the second lookup if provided
                    filter_kwargs = {lookup_field: lookup_value}
                    if lookup_field_2 and lookup_value_2:
                        filter_kwargs[lookup_field_2] = lookup_value_2
                    obj = model_cls._default_manager.get(**filter_kwargs)
                    target_id = str(obj.pk)
                except model_cls.DoesNotExist:
                    self.stderr.write(
                        f"No {target_model} matching {filter_kwargs}; skipping row '{legacy_value}'"
                    )
                    skipped += 1
                    continue
                except model_cls.MultipleObjectsReturned:
                    self.stderr.write(
                        f"Multiple {target_model} matching {filter_kwargs}; skipping row '{legacy_value}'"
                    )
                    skipped += 1
                    continue

                # target_id may be a numeric string or None at this point

                ct = self._resolve_content_type(target_model)
                if target_model and ct is None:
                    self.stderr.write(
                        f"Unknown model '{target_model}' in app '{APP_LABEL}'; skipping row '{legacy_value}'"
                    )
                    skipped += 1
                    continue

                defaults = {
                    "canonical_name": canonical,
                    "active": active,
                    "target_object_id": int(target_id) if target_id else None,
                    "target_content_type": ct,
                }

                if dry_run:
                    self.stdout.write(
                        f"[DRY] would create/update: list_name={row_list_name} {legacy_value} -> {defaults}"
                    )
                    continue

                obj, created_flag = LegacyValueMap.objects.get_or_create(
                    legacy_system=legacy_system,
                    list_name=row_list_name,
                    legacy_value=legacy_value,
                    defaults=defaults,
                )
                if created_flag:
                    created += 1
                else:
                    if do_update:
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        obj.save()
                        updated += 1

        self.stdout.write(
            f"created={created} updated={updated} skipped={skipped} (dry_run={dry_run})"
        )
