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
                elif isinstance(v, (list, tuple, set)):
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
            if isinstance(val, (list, tuple, set)):
                setattr(inst, field_name, [str(x) for x in val])
                return
            setattr(inst, field_name, [str(val)])
            return
        setattr(inst, field_name, val)
