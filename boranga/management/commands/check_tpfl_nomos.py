from __future__ import annotations

import csv
import os
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

    def handle(self, *args, **options):
        csv_path = options.get("csv")
        out_path = options.get("out")
        limit = options.get("limit")

        if not os.path.exists(csv_path):
            self.stderr.write(f"CSV not found: {csv_path}")
            return

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
            out_path = os.path.join(base_dir, f"check_tpfl_nomos_results_{ts}.csv")
        total = 0
        by_status = {"found": 0, "multiple": 0, "not_found": 0}
        rows_out = []

        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                total += 1
                if limit and total > limit:
                    break

                name = r.get("NAME")
                nomos_name = r.get("nomos_canonical_name") or ""
                nomos_id = r.get("nomos_taxon_id") or ""

                nomos_name_norm = _norm_name(nomos_name) if nomos_name else ""

                status = "not_found"
                details = ""

                # Try canonical name only (do not attempt nomos_taxon_id lookup)
                if nomos_name_norm:
                    # first exact match on stored value
                    try:
                        qs_exact = Taxonomy.objects.filter(
                            kingdom_fk__grouptype__name="flora",
                            scientific_name=nomos_name_norm,
                        )
                        cnt_exact = qs_exact.count()
                        if cnt_exact == 1:
                            status = "found"
                            details = f"pk={qs_exact.first().pk} (exact)"
                            by_status["found"] += 1
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue
                        elif cnt_exact > 1:
                            status = "multiple"
                            details = f"exact match count={cnt_exact}"
                            by_status["multiple"] += 1
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue

                        # try case-insensitive iexact
                        qs_ie = Taxonomy.objects.filter(
                            scientific_name__iexact=nomos_name_norm
                        )
                        cnt_ie = qs_ie.count()
                        if cnt_ie == 1:
                            status = "found"
                            details = f"pk={qs_ie.first().pk} (iexact)"
                            by_status["found"] += 1
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue
                        elif cnt_ie > 1:
                            status = "multiple"
                            details = f"iexact match count={cnt_ie}"
                            by_status["multiple"] += 1
                            rows_out.append(
                                (name, nomos_name, nomos_id, status, details)
                            )
                            continue
                    except Exception as e:
                        details = f"name lookup error: {e}"

                # nothing matched
                by_status["not_found"] = by_status.get("not_found", 0) + 1
                rows_out.append((name, nomos_name, nomos_id, status, details))

        # write results CSV
        with open(out_path, "w", newline="", encoding="utf-8") as outfh:
            w = csv.writer(outfh)
            w.writerow(
                ["NAME", "nomos_canonical_name", "nomos_taxon_id", "status", "details"]
            )
            for r in rows_out:
                w.writerow(r)

        # print summary
        self.stdout.write("\nTPFL nomos check complete")
        self.stdout.write(f"CSV: {csv_path}")
        self.stdout.write(f"Results written: {out_path}")
        self.stdout.write(f"Total rows processed: {total}")
        self.stdout.write(f"Found by name: {by_status.get('found',0)}")
        self.stdout.write(f"Multiple matches: {by_status.get('multiple',0)}")
        self.stdout.write(f"Not found: {by_status.get('not_found',0)}")

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
