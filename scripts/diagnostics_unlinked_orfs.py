import json
import os

# Run this via: python manage.py shell < scripts/diagnostics_unlinked_orfs.py

prefix = os.getenv("DIAG_PREFIX", "tfauna-")
group_type = os.getenv("DIAG_GROUP_TYPE", "fauna")

try:
    from django.core.exceptions import FieldDoesNotExist

    from boranga.components.occurrence.models import OccurrenceReport
except Exception as e:
    print(json.dumps({"error": f"import_failed: {e}"}))
    raise

# Build base queryset
qs = OccurrenceReport.objects.filter(migrated_from_id__istartswith=prefix)
# Attempt to apply group_type filter defensively
applied = False
if group_type:
    for fname in ("group_type", "group_type_id"):
        try:
            f = OccurrenceReport._meta.get_field(fname)
        except FieldDoesNotExist:
            continue
        internal = f.get_internal_type().lower()
        try:
            if internal in ("charfield", "textfield"):
                qs = qs.filter(**{f"{fname}__iexact": group_type})
                applied = True
                break
            if internal in ("foreignkey",):
                rel = f.related_model
                rel_field = None
                rel_field_names = {x.name for x in rel._meta.fields}
                for rn in ("code", "key", "slug", "name", "value"):
                    if rn in rel_field_names:
                        rel_field = rn
                        break
                if rel_field:
                    rel_obj = rel.objects.filter(**{f"{rel_field}__iexact": group_type}).first()
                    if rel_obj:
                        qs = qs.filter(**{f"{fname}": rel_obj.pk})
                        applied = True
                        break
                # Try numeric id fallback
                try:
                    ival = int(group_type)
                    qs = qs.filter(**{f"{fname}": ival})
                    applied = True
                    break
                except Exception:
                    pass
        except Exception:
            # Defensive: skip problematic filters
            continue

# If no group_type filter applied, leave qs as-is

total = qs.count()
linked = qs.exclude(occurrence_id__isnull=True).count()
unlinked_qs = qs.filter(occurrence_id__isnull=True)
unlinked = unlinked_qs.count()

# Choose available sample fields defensively
meta_fields = {f.name for f in OccurrenceReport._meta.fields}
want = ["migrated_from_id", "species_id", "occurrence_report_number", "id"]
avail = [f for f in want if f in meta_fields]

sample = list(unlinked_qs.values_list(*avail)[:200])

result = {
    "prefix": prefix,
    "group_type_filter": group_type,
    "group_type_filter_applied": applied,
    "total": total,
    "linked": linked,
    "unlinked": unlinked,
    "sample_unlinked_count": len(sample),
    "sample_unlinked": sample,
}

print(json.dumps(result, default=str, indent=2))
