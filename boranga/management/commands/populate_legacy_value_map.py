import csv

from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import transaction

from boranga.components.main.models import LegacyValueMap

APP_LABEL = "boranga"  # all target models live in this app


class Command(BaseCommand):
    """
    Example command:
        ./manage.py \
        populate_legacy_value_map private-media/legacy_data/TPFL/legacy-data-map-TPFL.csv \
            --legacy-system TPFL
    """

    help = (
        "Import LegacyValueMap rows from CSV. Columns: list_name, legacy_value, "
        "target_model, target_lookup_field_name, target_lookup_field_value, "
        "optional target_lookup_field_name_2/target_lookup_field_value_2, canonical_name, active"
    )

    def add_arguments(self, parser):
        parser.add_argument("csvfile", type=str)
        parser.add_argument("--legacy-system", required=True)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--update", action="store_true", help="update existing rows")
        parser.add_argument(
            "--create-missing-targets",
            action="store_true",
            help="create target model records if they don't exist (with warning)",
        )

    def _resolve_content_type(self, model_name: str | None) -> ContentType | None:
        if not model_name:
            return None
        model = model_name.strip().lower()
        try:
            return ContentType.objects.get(app_label=APP_LABEL, model=model)
        except ContentType.DoesNotExist:
            return None

    # Preferred lookup field names for specific models
    PREFERRED_LOOKUP_FIELDS = {
        "documentsubcategory": ["document_sub_category_name", "name"],
        "documentcategory": ["document_category_name", "name"],
        # Add more model-specific fields here as needed
    }

    def handle(self, *args, **options):
        csvfile = options["csvfile"]
        legacy_system = options["legacy_system"]
        dry_run = options["dry_run"]
        do_update = options["update"]
        create_missing = options["create_missing_targets"]

        rows = []
        with open(csvfile, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                rows.append(r)

        created = 0
        updated = 0
        skipped = 0
        targets_created = 0
        with transaction.atomic():
            for r in rows:
                legacy_value = (r.get("legacy_value") or r.get("legacy") or "").strip()
                if not legacy_value:
                    self.stderr.write("Skipping row with no legacy_value")
                    skipped += 1
                    continue

                # list_name is taken per-row from the CSV (required)
                row_list_name = (r.get("list_name") or r.get("list") or "").strip() or None
                if not row_list_name:
                    self.stderr.write(f"Missing list_name for legacy_value '{legacy_value}'; skipping")
                    skipped += 1
                    continue

                canonical = (r.get("canonical_name") or r.get("canonical") or "").strip()
                active = r.get("active", "true").strip().lower() not in (
                    "0",
                    "false",
                    "no",
                )

                # Optional: model and lookup field/value for model-specific mappings
                # When target_model is absent, this creates a simple value mapping
                target_model = (r.get("target_model") or r.get("model") or "").strip() or None

                target_id = None
                ct = None

                # Only perform model lookup if target_model is specified
                if target_model:
                    # default lookup field to preferred list for model, fallback to "name"
                    lookup_field = (r.get("target_lookup_field_name") or "").strip()
                    # No automatic remapping; require correct field names in CSV
                    if not lookup_field:
                        model_key = target_model.strip().lower()
                        preferred_fields = self.PREFERRED_LOOKUP_FIELDS.get(model_key, ["name"])
                        # Try each preferred field in order, use the first that exists
                        for pf in preferred_fields:
                            try:
                                model_cls = apps.get_model(APP_LABEL, model_key)
                                model_cls._meta.get_field(pf)
                                lookup_field = pf
                                break
                            except Exception:
                                continue
                        else:
                            lookup_field = "name"
                    lookup_value = (r.get("target_lookup_field_value") or "").strip() or None

                    # optional second lookup pair to further filter the target (parent/related)
                    # If provided, this will be added to the queryset filter as an additional key.
                    # Accepts related lookups using Django lookup syntax (e.g. "parent__name").
                    lookup_field_2 = (
                        r.get("target_lookup_field_name_2") or r.get("lookup_field_2") or ""
                    ).strip() or None
                    # No automatic remapping; require correct field names in CSV
                    lookup_value_2 = (
                        r.get("target_lookup_field_value_2") or r.get("lookup_value_2") or ""
                    ).strip() or None

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
                        # Use case-insensitive lookups for matching by appending '__iexact'
                        # unless an explicit lookup (iexact, icontains, etc.) is already provided.
                        from django.db.models import ForeignKey

                        def _ci_lookup_key(field_name: str, model_for_field=None) -> str:
                            # Check if the field is a ForeignKey - they don't support iexact
                            try:
                                field = model_cls._meta.get_field(field_name)
                                if isinstance(field, ForeignKey):
                                    return field_name + "__exact"
                            except Exception:
                                pass
                            # For other fields, use case-insensitive exact match
                            if field_name.endswith("__iexact"):
                                return field_name
                            return field_name + "__iexact"

                        # Handle the primary lookup field
                        try:
                            primary_field = model_cls._meta.get_field(lookup_field)
                            if isinstance(primary_field, ForeignKey):
                                related_model = primary_field.related_model
                                # Use preferred lookup field for related model
                                related_model_key = related_model.__name__.lower()
                                preferred_fields = self.PREFERRED_LOOKUP_FIELDS.get(related_model_key, ["name"])
                                rel_lookup_field = preferred_fields[0]
                                try:
                                    related_obj = related_model._default_manager.get(
                                        **{rel_lookup_field + "__iexact": lookup_value}
                                    )
                                    query_kwargs = {lookup_field + "_id": related_obj.pk}
                                    create_kwargs = {lookup_field + "_id": related_obj.pk}
                                except related_model.DoesNotExist:
                                    self.stderr.write(
                                        f"Related {related_model.__name__} with {rel_lookup_field}='{lookup_value}' "
                                        f"not found; skipping row '{legacy_value}'"
                                    )
                                    skipped += 1
                                    continue
                                except related_model.MultipleObjectsReturned:
                                    self.stderr.write(
                                        f"Multiple {related_model.__name__} with {rel_lookup_field}='{lookup_value}'; "
                                        f"skipping row '{legacy_value}'"
                                    )
                                    skipped += 1
                                    continue
                            else:
                                query_kwargs = {_ci_lookup_key(lookup_field): lookup_value}
                                create_kwargs = {lookup_field: lookup_value}
                        except Exception as e:
                            self.stderr.write(
                                f"Error determining field type for '{lookup_field}': {e}; skipping row '{legacy_value}'"
                            )
                            skipped += 1
                            continue

                        # Handle the second lookup field if provided
                        if lookup_field_2 and lookup_value_2:
                            try:
                                field_2 = model_cls._meta.get_field(lookup_field_2)
                                if isinstance(field_2, ForeignKey):
                                    related_model = field_2.related_model
                                    related_model_key = related_model.__name__.lower()
                                    preferred_fields_2 = self.PREFERRED_LOOKUP_FIELDS.get(related_model_key, ["name"])
                                    rel_lookup_field_2 = preferred_fields_2[0]
                                    try:
                                        related_obj = related_model._default_manager.get(
                                            **{rel_lookup_field_2 + "__iexact": lookup_value_2}
                                        )
                                        query_kwargs[lookup_field_2 + "_id"] = related_obj.pk
                                        create_kwargs[lookup_field_2 + "_id"] = related_obj.pk
                                    except related_model.DoesNotExist:
                                        self.stderr.write(
                                            f"Related {related_model.__name__} with {rel_lookup_field_2}='{lookup_value_2}' "
                                            f"not found; skipping row '{legacy_value}'"
                                        )
                                        skipped += 1
                                        continue
                                    except related_model.MultipleObjectsReturned:
                                        self.stderr.write(
                                            f"Multiple {related_model.__name__} with {rel_lookup_field_2}='{lookup_value_2}'; "
                                            f"skipping row '{legacy_value}'"
                                        )
                                        skipped += 1
                                        continue
                                else:
                                    query_kwargs[_ci_lookup_key(lookup_field_2)] = lookup_value_2
                                    create_kwargs[lookup_field_2] = lookup_value_2
                            except Exception as e:
                                self.stderr.write(
                                    f"Error determining field type for '{lookup_field_2}': {e}; "
                                    f"skipping row '{legacy_value}'"
                                )
                                skipped += 1
                                continue

                        obj = model_cls._default_manager.get(**query_kwargs)
                        target_id = str(obj.pk)
                    except model_cls.DoesNotExist:
                        if create_missing:
                            # Create the target object with a warning
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Creating missing {target_model} with {create_kwargs} "
                                    f"for legacy_value '{legacy_value}'"
                                )
                            )
                            try:
                                # Use a savepoint to prevent transaction pollution
                                with transaction.atomic():
                                    obj = model_cls._default_manager.create(**create_kwargs)
                                    target_id = str(obj.pk)
                                    targets_created += 1
                            except Exception as e:
                                self.stderr.write(
                                    f"Failed to create {target_model} with {create_kwargs}: "
                                    f"{e}; skipping row '{legacy_value}'"
                                )
                                skipped += 1
                                continue
                        else:
                            self.stderr.write(
                                f"No {target_model} matching {query_kwargs}; skipping row '{legacy_value}'"
                            )
                            skipped += 1
                            continue
                    except model_cls.MultipleObjectsReturned:
                        self.stderr.write(
                            f"Multiple {target_model} matching {query_kwargs}; skipping row '{legacy_value}'"
                        )
                        skipped += 1
                        continue

                    ct = self._resolve_content_type(target_model)
                    if ct is None:
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
            f"created={created} updated={updated} skipped={skipped} "
            f"targets_created={targets_created} (dry_run={dry_run})"
        )
