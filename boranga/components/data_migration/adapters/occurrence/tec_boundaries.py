import os

from django.conf import settings

from boranga.components.data_migration.registry import (
    static_value_factory,
    wkt_to_geometry_factory,
)

from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source

# Candidate filenames for the main WKT geometry file.
_WKT_FILE_CANDIDATES = [
    "tec_pec_boundaries_May_26_all_boundaries.csv",
    "TEC_PEC_Boundaries_Nov25.csv",
    "TEC_PEC_BOUNDARIES_NOV25.csv",
]

# Candidate filenames for the boundary-attribute table (BDY_ID, BDY_BUFFER).
# This is a separate file from the WKT file and is the authoritative source for BDY_BUFFER.
_BDY_ATTR_FILE_CANDIDATES = [
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


def _normalize_bdy_id(value) -> str:
    """Normalise a BDY_ID value to a plain integer string for cross-file joining.

    The main WKT file stores BDY_ID as e.g. ``'54.00000000000'`` while
    OCCURRENCES.csv stores it as ``'54'``.  Converting via int(float(...))
    makes the keys comparable.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


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
        Extract boundary data from tec_pec_boundaries_May_26_all_boundaries.csv (WKT geometry file).

        Three CSV files are involved:

        1. WKT file (``tec_pec_boundaries_May_26_all_boundaries.csv``): geometry rows.
           Columns of interest: ``WKT``, ``OCC_UNIQUE``, ``BDY_ID``.
        2. ``BOUNDARIES.csv``: boundary-attribute table.
           Columns: ``BDY_ID``, ``BDY_BUFFER``.  Joined via normalised BDY_ID.
        3. ``OCCURRENCES.csv``: occurrence table.
           Columns: ``BDY_ID``, ``OCC_UNIQUE_ID``, ``OCC_BUFFER_RADIUS``.

        buffer_radius priority logic
        ----------------------------
        Priority 1 – BDY_BUFFER from BOUNDARIES.csv (joined via BDY_ID):
            If BDY_BUFFER > 0, use it.
        Priority 2 – OCC_BUFFER_RADIUS from OCCURRENCES.csv (joined via BDY_ID):
            If BDY_BUFFER is null/0, use OCC_BUFFER_RADIUS.
        Fallback – None:
            If both are null/0, buffer_radius is set to None.
            These rows are emitted as QA warnings.
        """
        directory = path if os.path.isdir(path) else os.path.dirname(path)

        # ── Locate the WKT geometry file ───────────────────────────────────
        if os.path.isdir(path):
            wkt_path = self._find_file(directory, _WKT_FILE_CANDIDATES)
            if not wkt_path:
                return ExtractionResult(
                    rows=[],
                    warnings=[ExtractionWarning(f"TEC_BOUNDARIES: no WKT geometry file found in {path}")],
                )
            path = wkt_path

        warnings: list[ExtractionWarning] = []

        # ── Load BOUNDARIES.csv → bdy_id_to_bdy_buffer ────────────────────
        # Priority-1 source: BDY_BUFFER from the boundary-attribute table.
        bdy_attr_path = self._find_file(directory, _BDY_ATTR_FILE_CANDIDATES)
        bdy_id_to_bdy_buffer: dict[str, float | None] = {}
        if bdy_attr_path:
            attr_rows, attr_warns = self.read_table(bdy_attr_path, limit=0)
            warnings.extend(attr_warns)
            for r in attr_rows:
                bdy_id = _normalize_bdy_id(r.get("BDY_ID"))
                if bdy_id:
                    bdy_id_to_bdy_buffer[bdy_id] = _to_float_or_none(r.get("BDY_BUFFER"))
        else:
            warnings.append(
                ExtractionWarning(
                    f"TEC_BOUNDARIES: BOUNDARIES.csv not found in {directory}; "
                    "BDY_BUFFER (priority-1 buffer source) will be unavailable."
                )
            )

        # ── Load OCCURRENCES.csv → bdy_id_to_occ / occ_uid_to_buf ─────────
        # Priority-2 source: OCC_BUFFER_RADIUS; also provides OCC_UNIQUE_ID
        # for rows that don't carry OCC_UNIQUE directly in the WKT file.
        # NOTE: OCCURRENCES.csv is saved with a UTF-8 BOM; read with utf-8-sig
        # so the first column (OCC_UNIQUE_ID) is not corrupted by the \ufeff prefix.
        occurrences_path = self._find_file(directory, _OCCURRENCES_FILE_CANDIDATES)
        # bdy_id → (occ_unique_id_str, occ_buffer_radius_float_or_none)
        bdy_id_to_occ: dict[str, tuple[str, float | None]] = {}
        # occ_unique_id → occ_buffer_radius_float_or_none (direct lookup for BDY_ID=0 rows)
        occ_uid_to_buf: dict[str, float | None] = {}
        if occurrences_path:
            occ_rows, occ_warns = self.read_table(occurrences_path, encoding="utf-8-sig", limit=0)
            warnings.extend(occ_warns)
            for r in occ_rows:
                bdy_id = _normalize_bdy_id(r.get("BDY_ID"))
                occ_uid = str(r.get("OCC_UNIQUE_ID") or r.get("OCC_UNIQUE") or "").strip()
                if bdy_id and occ_uid:
                    buf = _to_float_or_none(r.get("OCC_BUFFER_RADIUS"))
                    bdy_id_to_occ[bdy_id] = (occ_uid, buf)
                if occ_uid:
                    occ_uid_to_buf[occ_uid] = _to_float_or_none(r.get("OCC_BUFFER_RADIUS"))
        else:
            warnings.append(
                ExtractionWarning(
                    f"TEC_BOUNDARIES: OCCURRENCES.csv not found in {directory}; "
                    "OCC_BUFFER_RADIUS fallback and OCC_UNIQUE_ID join will be skipped."
                )
            )

        # ── Read WKT geometry file ─────────────────────────────────────────
        raw_rows, wkt_warns = self.read_table(path)
        warnings.extend(wkt_warns)

        rows = []
        null_buffer_migrated_ids: list[str] = []

        for raw in raw_rows:
            row_num = raw.get("_row_num", "?")

            # ── Resolve OCC_UNIQUE_ID / migrated_from_id ───────────────────
            # OCC_UNIQUE is present directly in the WKT file.  If absent (older
            # file formats), fall back to BDY_ID join with OCCURRENCES.
            occ_unique = str(raw.get("OCC_UNIQUE") or raw.get("OCC_UNIQUE_ID") or "").strip()
            occ_buf_fallback: float | None = None

            bdy_id = _normalize_bdy_id(raw.get("BDY_ID"))

            if not occ_unique:
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
            # Priority 1: BDY_BUFFER from BOUNDARIES.csv (joined via BDY_ID).
            bdy_buffer = bdy_id_to_bdy_buffer.get(bdy_id) if bdy_id else None
            if bdy_buffer is not None:
                buffer_radius = bdy_buffer
            else:
                # Priority 2: OCC_BUFFER_RADIUS from OCCURRENCES.csv.
                # First try joining via BDY_ID (used when OCC_UNIQUE was resolved
                # from OCCURRENCES via BDY_ID, i.e. older WKT file formats).
                if occ_buf_fallback is None and bdy_id and bdy_id in bdy_id_to_occ:
                    occ_buf_fallback = bdy_id_to_occ[bdy_id][1]
                # For rows where BDY_ID is 0/absent (BDY_ID not in BOUNDARIES.csv),
                # fall back to a direct lookup by OCC_UNIQUE_ID in OCCURRENCES.csv.
                if occ_buf_fallback is None and occ_unique:
                    occ_buf_fallback = occ_uid_to_buf.get(occ_unique)
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
