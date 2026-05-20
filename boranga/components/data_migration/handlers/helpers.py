from __future__ import annotations

from typing import Any

from django.core.exceptions import FieldDoesNotExist
from django.db import models as dj_models


def normalize_create_kwargs(model_cls, kwargs: dict) -> dict:
    """Normalize kwargs for creating model instances.

    - Map ForeignKey fields to '<field>_id' when given an id or model instance.
    - Coerce MultiSelectField inputs into lists when necessary.
    """
    out = {}
    try:
        from multiselectfield.db.fields import MultiSelectField

        _MSF = MultiSelectField
    except Exception:
        _MSF = None
    for k, v in (kwargs or {}).items():
        if k.endswith("_id"):
            out[k] = v
            continue
        try:
            f = model_cls._meta.get_field(k)
        except FieldDoesNotExist:
            out[k] = v
            continue
        if isinstance(f, dj_models.ForeignKey):
            out_val = getattr(v, "pk", v)
            out[f"{k}_id"] = out_val
        else:
            if _MSF is not None and isinstance(f, _MSF):
                if v is None:
                    out[k] = v
                elif isinstance(v, str):
                    out[k] = [s.strip() for s in v.split(",") if s.strip()]
                elif isinstance(v, list | tuple | set):
                    out[k] = [str(x) for x in v]
                else:
                    out[k] = [str(v)]
            else:
                out[k] = v
    return out


def apply_value_to_instance(inst, field_name: str, val: Any):
    try:
        f = inst._meta.get_field(field_name)
    except FieldDoesNotExist:
        setattr(inst, field_name, val)
        return
    try:
        from multiselectfield.db.fields import MultiSelectField

        _MSF = MultiSelectField
    except Exception:
        _MSF = None
    if field_name.endswith("_id"):
        setattr(inst, field_name, getattr(val, "pk", val))
        return
    if isinstance(f, dj_models.ForeignKey):
        setattr(inst, f"{field_name}_id", getattr(val, "pk", val))
    else:
        if _MSF is not None and isinstance(f, _MSF):
            if val is None:
                setattr(inst, field_name, val)
                return
            if isinstance(val, str):
                val_list = [s.strip() for s in val.split(",") if s.strip()]
                setattr(inst, field_name, val_list)
                return
            if isinstance(val, list | tuple | set):
                setattr(inst, field_name, [str(x) for x in val])
                return
            setattr(inst, field_name, [str(val)])
            return
        setattr(inst, field_name, val)


def apply_model_defaults(model_cls, data: dict) -> dict:
    """
    Update data dict (in-place and returned) to replace None values with
    model defaults where applicable.
    """
    for k, v in list(data.items()):
        if v is not None:
            continue
        try:
            field = model_cls._meta.get_field(k)
        except FieldDoesNotExist:
            continue

        # Prefer explicit field default (handles callables)
        field_default = field.get_default()
        if field_default is not None:
            data[k] = field_default
            continue

        # Fallback: for non-nullable text fields, prefer empty string
        if not getattr(field, "null", False) and isinstance(field, dj_models.CharField | dj_models.TextField):
            data[k] = ""
            continue
    return data


def try_repair_geometry(geom):
    """Attempt to repair a topologically invalid GEOSGeometry using shapely.make_valid().

    Returns a tuple ``(repaired_geom, was_repaired, original_reason)`` where:
    - ``repaired_geom`` is the (possibly repaired) GEOSGeometry with the original SRID preserved.
    - ``was_repaired`` is True if the geometry was invalid and was successfully repaired.
    - ``original_reason`` is the GEOS validity reason string from before repair, or None.

    If ``geom`` is None, already valid, or cannot be repaired, the original is returned
    unchanged with ``was_repaired=False``.
    """
    if geom is None:
        return geom, False, None

    if geom.valid:
        return geom, False, None

    original_reason = geom.valid_reason
    try:
        import shapely
        import shapely.wkt
        from django.contrib.gis.geos import GEOSGeometry
        from shapely.validation import make_valid

        shapely_geom = shapely.wkt.loads(geom.wkt)
        repaired_shapely = make_valid(shapely_geom)
        repaired_geos = GEOSGeometry(repaired_shapely.wkt, srid=geom.srid)
        return repaired_geos, True, original_reason
    except Exception:
        return geom, False, original_reason
