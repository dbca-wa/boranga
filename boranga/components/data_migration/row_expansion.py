import re


def expand_children(schema, row: dict):
    """
    Returns (parent_row, {spec.name: [child_dicts,...]})
    """
    parent = dict(row)  # shallow copy
    children_map = {}
    specs = getattr(schema, "child_specs", None) or []
    for spec in specs:
        coll = []
        if spec.index_pattern:
            regex = re.compile(spec.index_pattern)
            buckets = {}
            remove_keys = []
            for k, v in row.items():
                m = regex.fullmatch(k)
                if not m:
                    continue
                idx, field = m.group(1), m.group(2)
                buckets.setdefault(idx, {})[field] = v
                remove_keys.append(k)
            for k in remove_keys:
                parent.pop(k, None)
            coll = [
                b
                for _, b in sorted(buckets.items(), key=lambda x: int(x[0]))
                if any(v not in (None, "", []) for v in b.values())
            ]
        if spec.split_columns:
            for col, cfg in spec.split_columns.items():
                raw = row.get(col)
                if not raw:
                    continue
                parts = [p.strip() for p in str(raw).split(cfg.get("sep", ";")) if p.strip()]
                for p in parts:
                    coll.append({cfg.get("field", col): p})
                parent.pop(col, None)
        if coll:
            children_map[spec.name] = coll
    return parent, children_map
