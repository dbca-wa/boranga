from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import datetime

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
        ./manage.py check_legacy_taxonomy_against_nomos \
        --csv boranga/components/data_migration/legacy_data/TPFL/TPFL_CS_LISTING_NAME_TO_NOMOS_CANONICAL_NAME.csv \
            --errors-only
    """

    help = "Check TPFL mapping CSV nomos_canonical_name/nomos_taxon_id values against Taxonomy"

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv",
            dest="csv",
            default=os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "components",
                "data_migration",
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
            help="Optional output CSV path (defaults to handler_output with timestamp)",
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
        iexact_lookup = defaultdict(
            list
        )  # scientific_name.lower() -> [Taxonomy objects]

        for tax in qs.iterator(chunk_size=1000):
            name = tax.scientific_name or ""
            exact_lookup[name].append(tax)
            iexact_lookup[name.lower()].append(tax)

        self.stdout.write(f"Cached {len(exact_lookup)} unique taxonomy names")

        # prepare output path
        if not out_path:
            base_dir = os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "components",
                "data_migration",
                "handlers",
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
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue
                        elif len(exact_matches) > 1:
                            status = "multiple"
                            details = f"exact match count={len(exact_matches)}"
                            by_status["multiple"] += 1
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue

                        # Check case-insensitive match
                        iexact_matches = iexact_lookup.get(nomos_name_norm.lower(), [])
                        if len(iexact_matches) == 1:
                            status = "found"
                            details = f"pk={iexact_matches[0].pk} (iexact)"
                            by_status["found"] += 1
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue
                        elif len(iexact_matches) > 1:
                            status = "multiple"
                            details = f"iexact match count={len(iexact_matches)}"
                            by_status["multiple"] += 1
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue
                    except Exception as e:
                        details = f"name lookup error: {e}"

                # nothing matched
                by_status["not_found"] += 1
                rows_out.append((name, nomos_name, nomos_id, status, details))

        # Show final count after loop
        self.stdout.write(f"Finished processing {total} rows")

        # write results CSV
        written_count = 0
        with open(out_path, "w", newline="", encoding="utf-8") as outfh:
            w = csv.writer(outfh)
            w.writerow(
                ["NAME", "nomos_canonical_name", "nomos_taxon_id", "status", "details"]
            )
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
                self.stdout.write(
                    f"- NAME={s[0]!r} nomos_canonical_name={s[1]!r} nomos_taxon_id={s[2]!r}"
                )

        # final
        return
