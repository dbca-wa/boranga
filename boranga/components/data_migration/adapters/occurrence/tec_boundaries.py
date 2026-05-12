import os

from django.conf import settings

from boranga.components.data_migration.registry import (
    static_value_factory,
    wkt_to_geometry_factory,
)

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source

# Candidate filenames for the BOUNDARIES source file (case-insensitive fallback handled below).
_BOUNDARIES_FILE_CANDIDATES = [
    "TEC_PEC_Boundaries_Nov25.csv",
    "TEC_PEC_BOUNDARIES_NOV25.csv",
    "BOUNDARIES.csv",
    "boundaries.csv",
]

# Candidate filenames for the OCCURRENCES source file (needed for BDY_ID → OCC_UNIQUE_ID
# cross-reference and to obtain the fallback OCC_BUFFER_RADIUS value).
_OCCURRENCES_FILE_CANDIDATES = [
    "OCCURRENCES.csv",
    "occurrences.csv",
]


def _to_float_or_none(value):
    """Convert *value* to float; return None if it is absent, blank, or zero."""
    if value is None:
        return None
    try:
        f = float(str(value).strip())
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


class OccurrenceTecBoundariesAdapter(SourceAdapter):
    source_key = Source.TEC_BOUNDARIES.value
    domain = "occurrence"

    # Pipelines for the geometry record
    PIPELINES = {
        # Transform WKT (Source GDA94/4283 -> Target DEFAULT_SRID)
        "OccurrenceGeometry__geometry": [wkt_to_geometry_factory(source_srid=4283, target_srid=settings.DEFAULT_SRID)],
        "OccurrenceGeometry__drawn_by": [static_value_factory(None)],
        "OccurrenceGeometry__locked": [static_value_factory(False)],
        # buffer_radius is populated in extract() via priority logic; no pipeline transform needed
        # (the value is already a float or None by the time it leaves extract()).
    }

    def _find_file(self, directory, candidates):
        """Return the first existing file from *candidates* inside *directory*, or None."""
        for name in candidates:
            p = os.path.join(directory, name)
            if os.path.exists(p):
                return p
        return None

    def extract(self, path: str, **options) -> ExtractionResult:
        """
        Extract boundary data from TEC_PEC_Boundaries.csv (or BOUNDARIES.csv).

        buffer_radius priority logic
        ----------------------------
        Priority 1 – BDY_BUFFER from BOUNDARIES (this file):
            If BDY_BUFFER > 0, use it (string → float conversion).
        Priority 2 – OCC_BUFFER_RADIUS from OCCURRENCES.csv:
            If BDY_BUFFER is null/0, look up OCC_BUFFER_RADIUS via the
            BDY_ID → OCC_UNIQUE_ID join with OCCURRENCES.csv.
        Fallback – None:
            If both are null/0, buffer_radius is set to None.
            These rows are reported as QA warnings for S&C review.

        Cross-reference
        ---------------
        BOUNDARIES does not carry OCC_UNIQUE_ID directly; instead BDY_ID is
        used as the join key between BOUNDARIES and OCCURRENCES.  The
        OCC_UNIQUE / OCC_UNIQUE_ID column in the BOUNDARIES file is also
        accepted when present for backward-compatibility.
        """
        directory = path if os.path.isdir(path) else os.path.dirname(path)

        # ── Locate the BOUNDARIES file ─────────────────────────────────────
        if os.path.isdir(path):
            boundaries_path = self._find_file(directory, _BOUNDARIES_FILE_CANDIDATES)
            if not boundaries_path:
                return ExtractionResult(
                    rows=[],
                    warnings=[ExtractionWarning(f"TEC_BOUNDARIES: no boundaries file found in {path}")],
                )
            path = boundaries_path

        # ── Locate and load OCCURRENCES.csv for the BDY_ID join ────────────
        occurrences_path = self._find_file(directory, _OCCURRENCES_FILE_CANDIDATES)
        # bdy_id → (occ_unique_id_str, occ_buffer_radius_float_or_none)
        bdy_id_to_occ: dict[str, tuple[str, float | None]] = {}
        warnings: list[ExtractionWarning] = []

        if occurrences_path:
            occ_rows, occ_warns = self.read_table(occurrences_path, limit=0)
            warnings.extend(occ_warns)
            for r in occ_rows:
                bdy_id = str(r.get("BDY_ID") or "").strip()
                occ_uid = str(r.get("OCC_UNIQUE_ID") or r.get("OCC_UNIQUE") or "").strip()
                if bdy_id and occ_uid:
                    buf = _to_float_or_none(r.get("OCC_BUFFER_RADIUS"))
                    bdy_id_to_occ[bdy_id] = (occ_uid, buf)
        else:
            warnings.append(
                ExtractionWarning(
                    f"TEC_BOUNDARIES: OCCURRENCES.csv not found in {directory}; "
                    "BDY_ID -> OCC_UNIQUE_ID join and OCC_BUFFER_RADIUS fallback will be skipped."
                )
            )

        # ── Read BOUNDARIES file ────────────────────────────────────────────
        raw_rows, bdy_warns = self.read_table(path)
        warnings.extend(bdy_warns)

        rows = []
        null_buffer_migrated_ids: list[str] = []

        for raw in raw_rows:
            row_num = raw.get("_row_num", "?")

            # ── Resolve OCC_UNIQUE_ID / migrated_from_id ───────────────────
            # Accept OCC_UNIQUE / OCC_UNIQUE_ID directly in the BOUNDARIES file
            # (backward compat), otherwise join via BDY_ID from OCCURRENCES.
            occ_unique = str(raw.get("OCC_UNIQUE") or raw.get("OCC_UNIQUE_ID") or "").strip()
            occ_buf_fallback: float | None = None

            if not occ_unique:
                bdy_id = str(raw.get("BDY_ID") or "").strip()
                if bdy_id and bdy_id in bdy_id_to_occ:
                    occ_unique, occ_buf_fallback = bdy_id_to_occ[bdy_id]
                else:
                    if bdy_id:
                        warnings.append(
                            ExtractionWarning(
                                f"TEC_BOUNDARIES row {row_num}: BDY_ID={bdy_id!r} not found in OCCURRENCES; skipping."
                            )
                        )
                    else:
                        warnings.append(
                            ExtractionWarning(f"TEC_BOUNDARIES row {row_num}: missing OCC_UNIQUE and BDY_ID; skipping.")
                        )
                    continue

            migrated_id = f"tec-{occ_unique}"

            # ── Require WKT ────────────────────────────────────────────────
            wkt = raw.get("WKT")
            if not wkt:
                continue

            # ── buffer_radius priority logic ───────────────────────────────
            # Priority 1: BDY_BUFFER from this BOUNDARIES row
            bdy_buffer = _to_float_or_none(raw.get("BDY_BUFFER"))
            if bdy_buffer is not None:
                buffer_radius = bdy_buffer
            else:
                # Priority 2: OCC_BUFFER_RADIUS from OCCURRENCES join
                if occ_buf_fallback is None:
                    # If we got occ_uid directly from the row (backward compat path),
                    # look up OCC_BUFFER_RADIUS by BDY_ID from the join table.
                    bdy_id_lookup = str(raw.get("BDY_ID") or "").strip()
                    if bdy_id_lookup and bdy_id_lookup in bdy_id_to_occ:
                        occ_buf_fallback = bdy_id_to_occ[bdy_id_lookup][1]
                buffer_radius = occ_buf_fallback  # may still be None

            if buffer_radius is None:
                null_buffer_migrated_ids.append(migrated_id)

            row = {
                "migrated_from_id": migrated_id,
                "OccurrenceGeometry__geometry": wkt,
                "OccurrenceGeometry__buffer_radius": buffer_radius,
            }
            rows.append(row)

        # ── QA warning for null buffer_radius rows ─────────────────────────
        if null_buffer_migrated_ids:
            warnings.append(
                ExtractionWarning(
                    f"TEC_BOUNDARIES: {len(null_buffer_migrated_ids)} occurrence(s) have buffer_radius=null "
                    f"(both BDY_BUFFER and OCC_BUFFER_RADIUS are absent/zero). "
                    f"QA review required. IDs: {', '.join(null_buffer_migrated_ids[:50])}"
                    + (" ... (truncated)" if len(null_buffer_migrated_ids) > 50 else "")
                )
            )

        return ExtractionResult(rows=rows, warnings=warnings)
