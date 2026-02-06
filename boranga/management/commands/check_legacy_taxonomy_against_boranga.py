from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand

from boranga.components.species_and_communities.models import Taxonomy

# try to import neutralise_html helper if available
try:
    from boranga.components.main.models import neutralise_html
except Exception:
    neutralise_html = None


def _norm_name(s: str) -> str:
    if s is None:
        return ""
    # normalise NBSPs and narrow NBSPs to spaces and strip
    s2 = str(s).replace("\u00a0", " ").replace("\u202f", " ").strip()
    if neutralise_html:
        try:
            return neutralise_html(s2)
        except Exception:
            return s2
    return s2


class Command(BaseCommand):
    """
    Example usage:
        ./manage.py check_legacy_taxonomy_against_boranga \
        --csv private-media/legacy_data/TPFL/TPFL_CS_LISTING_NAME_TO_NOMOS_CANONICAL_NAME.csv \
            --group-type flora --errors-only
    """

    help = "Check TPFL mapping CSV nomos_canonical_name/nomos_taxon_id values against Taxonomy"

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv",
            dest="csv",
            default=os.path.join(
                settings.BASE_DIR,
                "private-media",
                "legacy_data",
                "TPFL",
                "TPFL_CS_LISTING_NAME_TO_NOMOS_CANONICAL_NAME.csv",
            ),
            help="Path to TPFL CSV mapping file",
        )
        parser.add_argument(
            "--out",
            dest="out",
            default=None,
            help="Optional output CSV path (defaults to private-media/handler_output with timestamp)",
        )
        parser.add_argument(
            "--limit",
            dest="limit",
            type=int,
            default=None,
            help="Optional limit rows to process (for testing)",
        )
        parser.add_argument(
            "--errors-only",
            dest="errors_only",
            action="store_true",
            default=False,
            help="If set, only write rows with errors (non-found) to the output CSV",
        )
        parser.add_argument(
            "--group-type",
            dest="group_type",
            default=None,
            help=(
                "Optional group type name to filter taxonomy lookups (e.g., flora, fauna, "
                "community). If omitted, no group-type filter is applied."
            ),
        )

    def handle(self, *args, **options):
        csv_path = options.get("csv")
        out_path = options.get("out")
        limit = options.get("limit")
        errors_only = options.get("errors_only", False)
        group_type = options.get("group_type")

        if not os.path.exists(csv_path):
            self.stderr.write(f"CSV not found: {csv_path}")
            return

        # **OPTIMIZATION 1: Build lookup cache upfront**
        self.stdout.write("Building taxonomy lookup cache...")

        qs = Taxonomy.objects.all()
        if group_type:
            qs = qs.filter(kingdom_fk__grouptype__name=group_type)

        # Build indices for fast lookups
        exact_lookup = defaultdict(list)  # scientific_name -> [Taxonomy objects]
        iexact_lookup = defaultdict(list)  # scientific_name.lower() -> [Taxonomy objects]

        for tax in qs.iterator(chunk_size=1000):
            name = tax.scientific_name or ""
            # normalize stored taxonomy names for lookup (preserve original and a
            # variant that replaces Unicode multiplication sign '×' with ASCII 'x')
            name_key = _norm_name(name)
            exact_lookup[name_key].append(tax)
            iexact_lookup[name_key.lower()].append(tax)

            # Add hybrid-marker variant so '×' vs 'x' differences don't prevent matches
            if "×" in name_key:
                alt = name_key.replace("×", "x")
                if alt != name_key:
                    exact_lookup[alt].append(tax)
                    iexact_lookup[alt.lower()].append(tax)

        self.stdout.write(f"Cached {len(exact_lookup)} unique taxonomy names")

        # prepare output path
        if not out_path:
            base_dir = os.path.join(
                settings.BASE_DIR,
                "private-media",
                "handler_output",
            )
            os.makedirs(base_dir, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            csv_base = os.path.splitext(os.path.basename(csv_path))[0]
            if errors_only:
                prefix = f"{csv_base}_errors"
            else:
                prefix = f"{csv_base}_results"
            out_path = os.path.join(base_dir, f"{prefix}_{ts}.csv")

        total = 0
        by_status = {"found": 0, "multiple": 0, "not_found": 0}
        rows_out = []

        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                total += 1
                if limit and total > limit:
                    break

                # Show progress every 1000 rows (moved to top of loop)
                if total % 1000 == 0:
                    self.stdout.write(f"Processed {total} rows...")

                name = r.get("NAME")
                nomos_name = r.get("nomos_canonical_name") or ""
                nomos_id = r.get("nomos_taxon_id") or ""

                nomos_name_norm = _norm_name(nomos_name) if nomos_name else ""

                status = "not_found"
                details = ""

                # **OPTIMIZATION 2: Use in-memory lookups instead of DB queries**
                if nomos_name_norm:
                    try:
                        # Check exact match
                        exact_matches = exact_lookup.get(nomos_name_norm, [])
                        if len(exact_matches) == 1:
                            status = "found"
                            details = f"pk={exact_matches[0].pk} (exact)"
                            by_status["found"] += 1
                            rows_out.append((name, nomos_name, nomos_id, status, details))
                            continue
                        elif len(exact_matches) > 1:
                            # If one of the matches is marked is_current=True, prefer it
                            current_matches = [t for t in exact_matches if getattr(t, "is_current", False)]
                            if len(current_matches) == 1:
                                status = "found"
                                details = f"pk={current_matches[0].pk} (exact, is_current)"
                                by_status["found"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue
                            # Otherwise treat as multiple
                            status = "multiple"
                            details = f"exact match count={len(exact_matches)}"
                            by_status["multiple"] += 1
                            rows_out.append((name, nomos_name, nomos_id, status, details))
                            continue

                        # Check case-insensitive match
                        iexact_matches = iexact_lookup.get(nomos_name_norm.lower(), [])
                        if len(iexact_matches) == 1:
                            status = "found"
                            details = f"pk={iexact_matches[0].pk} (iexact)"
                            by_status["found"] += 1
                            rows_out.append((name, nomos_name, nomos_id, status, details))
                            continue
                        elif len(iexact_matches) > 1:
                            # If one of the case-insensitive matches is marked is_current=True, prefer it
                            current_matches = [t for t in iexact_matches if getattr(t, "is_current", False)]
                            if len(current_matches) == 1:
                                status = "found"
                                details = f"pk={current_matches[0].pk} (iexact, is_current)"
                                by_status["found"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue
                            # Otherwise treat as multiple
                            status = "multiple"
                            details = f"iexact match count={len(iexact_matches)}"
                            by_status["multiple"] += 1
                            rows_out.append((name, nomos_name, nomos_id, status, details))
                            continue
                    except Exception as e:
                        details = f"name lookup error: {e}"

                # If nomos_canonical_name was empty, fall back to trying the CSV `NAME`
                # (some rows only have NAME populated). This tries full-name exact/iexact
                # matches before attempting the special ' PN' suffix strip below.
                if status != "found" and not nomos_name_norm and name:
                    try:
                        name_candidate = _norm_name(name)
                        if name_candidate:
                            # Check exact match using NAME
                            exact_matches = exact_lookup.get(name_candidate, [])
                            if len(exact_matches) == 1:
                                status = "found"
                                details = f"pk={exact_matches[0].pk} (NAME exact)"
                                by_status["found"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue
                            elif len(exact_matches) > 1:
                                current_matches = [t for t in exact_matches if getattr(t, "is_current", False)]
                                if len(current_matches) == 1:
                                    status = "found"
                                    details = f"pk={current_matches[0].pk} (NAME exact, is_current)"
                                    by_status["found"] += 1
                                    rows_out.append((name, nomos_name, nomos_id, status, details))
                                    continue
                                status = "multiple"
                                details = f"NAME exact match count={len(exact_matches)}"
                                by_status["multiple"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue

                            # Check case-insensitive match for NAME
                            iexact_matches = iexact_lookup.get(name_candidate.lower(), [])
                            if len(iexact_matches) == 1:
                                status = "found"
                                details = f"pk={iexact_matches[0].pk} (NAME iexact)"
                                by_status["found"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue
                            elif len(iexact_matches) > 1:
                                current_matches = [t for t in iexact_matches if getattr(t, "is_current", False)]
                                if len(current_matches) == 1:
                                    status = "found"
                                    details = f"pk={current_matches[0].pk} (NAME iexact, is_current)"
                                    by_status["found"] += 1
                                    rows_out.append((name, nomos_name, nomos_id, status, details))
                                    continue
                                status = "multiple"
                                details = f"NAME iexact match count={len(iexact_matches)}"
                                by_status["multiple"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue
                    except Exception as e:
                        details = f"name lookup error: {e}"

                # If nothing matched so far and the CSV `NAME` ends with ' PN',
                # try again with that suffix removed (handles provincial suffixes).
                if status != "found" and name:
                    try:
                        name_strip = str(name).strip()
                    except Exception:
                        name_strip = ""

                    if name_strip.endswith(" PN"):
                        alt_candidate = _norm_name(name_strip[:-3])
                        if alt_candidate:
                            # Check exact match for alt candidate
                            alt_exact = exact_lookup.get(alt_candidate, [])
                            if len(alt_exact) == 1:
                                status = "found"
                                details = f"pk={alt_exact[0].pk} (NAME alt exact strip ' PN')"
                                by_status["found"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue
                            elif len(alt_exact) > 1:
                                current_matches = [t for t in alt_exact if getattr(t, "is_current", False)]
                                if len(current_matches) == 1:
                                    status = "found"
                                    details = f"pk={current_matches[0].pk} (NAME alt exact is_current strip ' PN')"
                                    by_status["found"] += 1
                                    rows_out.append((name, nomos_name, nomos_id, status, details))
                                    continue
                                status = "multiple"
                                details = f"alt exact match count={len(alt_exact)}"
                                by_status["multiple"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue

                            # Check case-insensitive for alt candidate
                            alt_iexact = iexact_lookup.get(alt_candidate.lower(), [])
                            if len(alt_iexact) == 1:
                                status = "found"
                                details = f"pk={alt_iexact[0].pk} (NAME alt iexact strip ' PN')"
                                by_status["found"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue
                            elif len(alt_iexact) > 1:
                                current_matches = [t for t in alt_iexact if getattr(t, "is_current", False)]
                                if len(current_matches) == 1:
                                    status = "found"
                                    details = f"pk={current_matches[0].pk} (NAME alt iexact is_current strip ' PN')"
                                    by_status["found"] += 1
                                    rows_out.append((name, nomos_name, nomos_id, status, details))
                                    continue
                                status = "multiple"
                                details = f"alt iexact match count={len(alt_iexact)}"
                                by_status["multiple"] += 1
                                rows_out.append((name, nomos_name, nomos_id, status, details))
                                continue

                # nothing matched
                by_status["not_found"] += 1
                rows_out.append((name, nomos_name, nomos_id, status, details))

        # Show final count after loop
        self.stdout.write(f"Finished processing {total} rows")

        # write results CSV
        written_count = 0
        with open(out_path, "w", newline="", encoding="utf-8") as outfh:
            w = csv.writer(outfh)
            w.writerow(["NAME", "nomos_canonical_name", "nomos_taxon_id", "status", "details"])
            for r in rows_out:
                # if errors_only is set, skip rows where status is 'found'
                if errors_only and r[3] == "found":
                    continue
                w.writerow(r)
                written_count += 1

        # print summary
        self.stdout.write("\nTPFL nomos check complete")
        self.stdout.write(f"CSV: {csv_path}")
        # include how many rows were written to the output
        try:
            self.stdout.write(f"Results written: {out_path} ({written_count} rows)")
        except Exception:
            # fallback if written_count not available for any reason
            self.stdout.write(f"Results written: {out_path}")
        self.stdout.write(f"Total rows processed: {total}")
        self.stdout.write(f"Found by name: {by_status.get('found', 0)}")
        self.stdout.write(f"Multiple matches: {by_status.get('multiple', 0)}")
        self.stdout.write(f"Not found: {by_status.get('not_found', 0)}")

        # also emit top 20 unmatched name samples
        not_found_samples = [r for r in rows_out if r[3] == "not_found"]
        if not_found_samples:
            self.stdout.write("\nSample not-found names (up to 20):")
            for s in not_found_samples[:20]:
                self.stdout.write(f"- NAME={s[0]!r} nomos_canonical_name={s[1]!r} nomos_taxon_id={s[2]!r}")

        # Also emit a sample of rows that had multiple matches
        multiple_samples = [r for r in rows_out if r[3] == "multiple"]
        if multiple_samples:
            self.stdout.write("\nSample multiple-match names (up to 20):")
            for s in multiple_samples[:20]:
                # include the details field (e.g., match counts) for troubleshooting
                self.stdout.write(
                    f"- NAME={s[0]!r} nomos_canonical_name={s[1]!r} nomos_taxon_id={s[2]!r} details={s[4]!r}"
                )

        # final
        return
