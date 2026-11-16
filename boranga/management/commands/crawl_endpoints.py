# your_app/management/commands/crawl_endpoints.py
import logging
import re
import warnings
from collections import Counter

from django.apps import apps
from django.conf import settings
from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY, get_user_model
from django.core.management.base import BaseCommand
from django.test import Client
from django.urls import URLPattern, URLResolver, get_resolver

try:
    import openpyxl
    from openpyxl.styles import Alignment, Font, PatternFill
except ImportError:
    openpyxl = None

IGNORE_PREFIXES = [
    "/admin/reversion/revision/",
    "/api/schema",
    "/api/docs",
    "/swagger",
    "/redoc",
    # Media and proxy routes that don't serve HTML/API views
    "/media/",
    "/private-media/",
    "/geoproxy/",
]

IGNORE_PATTERNS = [
    r"/\d+/history/$",
    r"/\d+/delete/$",
    r"/\d+/change/$",
    r"/admin/.+/\d+/$",
    r"/ledger-api/.+/",
    r"/ledger-toolkit-api/.+/",
    r"/ledger-ui/.+/",
    r"/admin/ledger_api_client/.+/",
    r"/move-[^/]+/?$",
    r"^/admin/1/?$",
    r"^/api/history/",
]

EXPECTED_FAILURES = [
    "/sentry-debug/",
]


def normalize_name(name):
    return re.sub(r"[^a-zA-Z0-9]", "", str(name)).lower()


class MemoryLogHandler(logging.Handler):
    """Captures ERROR log records emitted during a single request."""

    def __init__(self):
        super().__init__()
        self.buffer = []

    def emit(self, record):
        self.buffer.append(self.format(record))

    def get_and_clear(self):
        content = "\n".join(self.buffer).strip()
        self.buffer.clear()
        return content


class Command(BaseCommand):
    help = "Crawls all GET endpoints and writes tracebacks to crawl_errors.log"

    def add_arguments(self, parser):
        parser.add_argument("--output", default="crawl_results.xlsx", help="XLSX report filename")
        parser.add_argument("--log-file", default="crawl_errors.log", help="Error log filename")

    def build_model_lookup(self):
        """Indexes all models by normalized names and tables."""
        lookup = {}
        for model in apps.get_models():
            cls_norm = normalize_name(model.__name__)
            model_norm = normalize_name(model._meta.model_name)
            table_norm = normalize_name(model._meta.db_table)

            lookup[cls_norm] = model
            lookup[model_norm] = model
            lookup[table_norm] = model
            lookup[cls_norm + "s"] = model
            lookup[model_norm + "s"] = model

        return lookup

    def get_model_and_lookup_field(self, callback, prev_segment=None):
        """Discovers the model and lookup field from the callback ViewSet or URL segment."""
        view_cls = getattr(callback, "cls", None) or getattr(callback, "view_class", None)
        model = None
        lookup_field = "pk"

        if view_cls:
            lookup_field = getattr(view_cls, "lookup_field", "pk")

            # 1. Direct class queryset
            qs = getattr(view_cls, "queryset", None)
            if qs is not None and hasattr(qs, "model"):
                model = qs.model

            # 2. Direct model attribute
            if not model and hasattr(view_cls, "model") and view_cls.model:
                model = view_cls.model

            # 3. Serializer class Meta.model
            if not model:
                serializer_cls = getattr(view_cls, "serializer_class", None)
                if serializer_cls and hasattr(serializer_cls, "Meta"):
                    model = getattr(serializer_cls.Meta, "model", None)

            # 4. ViewSet class name (e.g. SpeciesViewSet -> Species)
            if not model:
                cleaned_name = re.sub(
                    r"(ViewSet|DetailView|UpdateView|DeleteView|View|APIView|ListCreateAPIView)$",
                    "",
                    view_cls.__name__,
                )
                model = self.model_lookup.get(normalize_name(cleaned_name))

        # 5. Fallback: match preceding URL path segment (e.g. /species/<id>/)
        if not model and prev_segment:
            norm_seg = normalize_name(prev_segment)
            model = self.model_lookup.get(norm_seg)
            if not model and norm_seg.endswith("s"):
                model = self.model_lookup.get(norm_seg[:-1])

        return model, lookup_field

    def get_real_id(self, model, lookup_field, cache):
        """
        Queries the database for an actual record and extracts its lookup value.
        Prefers active/non-discarded records.
        """
        if not model:
            return None, False

        if model not in cache:
            try:
                qs = model.objects.all()

                # Filter out discarded/archived states if the field exists
                field_names = [f.name for f in model._meta.fields]
                if "processing_status" in field_names:
                    qs = qs.exclude(
                        processing_status__in=[
                            "discarded",
                            "recalled",
                            "historical",
                            "closed",
                        ]
                    )
                if "is_deleted" in field_names:
                    qs = qs.filter(is_deleted=False)

                # Fetch first valid record
                obj = qs.first() or model.objects.first()

                if obj:
                    val = getattr(obj, lookup_field, None) or obj.pk
                    cache[model] = (str(val), False)
                else:
                    cache[model] = (None, True)  # Table has 0 records
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"ERROR resolving ID for {model.__name__} (lookup: {lookup_field}): {e}")
                )
                cache[model] = (None, False)

        return cache[model]

    def handle(self, *args, **options):
        output_file = options["output"]
        error_log_file = options["log_file"]

        logging.disable(logging.WARNING)
        warnings.filterwarnings("ignore")

        memory_handler = MemoryLogHandler()
        memory_handler.setLevel(logging.ERROR)
        memory_handler.setFormatter(logging.Formatter("%(levelname)s [%(name)s]: %(message)s"))
        root_logger = logging.getLogger()
        root_logger.addHandler(memory_handler)

        client = Client()
        self.model_lookup = self.build_model_lookup()

        User = get_user_model()
        user = User.objects.filter(is_superuser=True).first()
        if user:
            session = client.session
            session[SESSION_KEY] = user._meta.pk.value_to_string(user)
            session[BACKEND_SESSION_KEY] = settings.AUTHENTICATION_BACKENDS[0]
            session[HASH_SESSION_KEY] = user.get_session_auth_hash()
            session.save()
            self.stdout.write(self.style.SUCCESS(f"Logged in as {user}"))

        # 1. Extract all URL routes with callbacks
        raw_routes = []

        def extract_urls(url_list, prefix=""):
            for entry in url_list:
                if isinstance(entry, URLPattern):
                    raw_routes.append((prefix + str(entry.pattern), entry.callback))
                elif isinstance(entry, URLResolver):
                    extract_urls(entry.url_patterns, prefix + str(entry.pattern))

        extract_urls(get_resolver().url_patterns)

        # 2. Parameter replacement via direct model inspection
        model_id_cache = {}
        cleaned_endpoints = {}

        for route, callback in raw_routes:
            if "format>" in route or "<format>" in route:
                continue

            # Extract preceding segment safely
            simplified = re.sub(r"\(\?P<[^>]+>.*?\)", "__PARAM__", route)
            simplified = re.sub(r"<.*?>", "__PARAM__", simplified)
            segments = [s.strip("^$/\\") for s in simplified.split("/") if s.strip("^$/\\")]

            param_indices = [i for i, s in enumerate(segments) if "__PARAM__" in s]
            prev_segment = segments[param_indices[0] - 1] if param_indices and param_indices[0] > 0 else None

            model, lookup_field = self.get_model_and_lookup_field(callback, prev_segment=prev_segment)
            real_id, is_empty_table = self.get_real_id(model, lookup_field, model_id_cache)

            replacement_val = real_id if real_id else ("NO_DATA" if is_empty_table else "1")

            cleaned = re.sub(r"<.*?>", replacement_val, route)
            cleaned = re.sub(r"\(\?P<.*?>.*?\)", replacement_val, cleaned)
            cleaned = re.sub(r"\([^)]*?\)", replacement_val, cleaned)
            cleaned = cleaned.replace("^", "").replace("$", "").replace("\\", "")
            cleaned = re.sub(r"/\?", "/", cleaned)
            cleaned = re.sub(r"\[[^\]]+\]\+?", "", cleaned)
            cleaned = "/" + re.sub(r"/+", "/", cleaned).lstrip("/")

            cleaned_endpoints[cleaned] = {
                "model": model,
                "is_empty": is_empty_table,
                "used_id": real_id,
            }

        patterns = sorted(list(cleaned_endpoints.keys()))
        self.stdout.write(f"Crawling {len(patterns)} endpoints...\n")

        results = []
        error_log_entries = []

        for path in patterns:
            meta = cleaned_endpoints[path]
            model = meta["model"]

            if any(path.startswith(prefix) for prefix in IGNORE_PREFIXES):
                self.stdout.write(self.style.WARNING(f"SKIP : {path}"))
                results.append([path, "N/A", "SKIPPED", "In IGNORE_PREFIXES"])
                continue

            if any(re.search(pattern, path) for pattern in IGNORE_PATTERNS):
                self.stdout.write(self.style.WARNING(f"SKIP : {path}"))
                results.append([path, "N/A", "SKIPPED", "Matched ignore pattern"])
                continue

            # Safely skip when the database table has 0 records
            if meta["is_empty"]:
                m_name = model.__name__ if model else "Model"
                self.stdout.write(self.style.WARNING(f"SKIP : {path} [Table '{m_name}' has 0 rows in DB]"))
                results.append([path, "N/A", "SKIPPED", f"Table {m_name} has 0 records in database"])
                continue

            is_expected_fail = any(path.startswith(prefix) for prefix in EXPECTED_FAILURES)
            memory_handler.get_and_clear()

            try:
                response = client.get(path, follow=False)
                status = response.status_code
                captured_traceback = memory_handler.get_and_clear()

                if status >= 500:
                    if is_expected_fail:
                        self.stdout.write(self.style.WARNING(f"EXP-FAIL [{status}]: {path}"))
                        results.append([path, status, "EXPECTED_FAIL", "HTTP 500 (Expected)"])
                    else:
                        self.stdout.write(self.style.ERROR(f"FAIL [{status}]: {path}"))
                        results.append([path, status, "FAIL", captured_traceback[:150] or "HTTP 500"])
                        error_log_entries.append(
                            f"{'=' * 80}\nURL: {path} [HTTP {status}]\n{'=' * 80}\n"
                            f"{captured_traceback or 'No traceback captured.'}\n\n"
                        )
                elif status == 404:
                    self.stdout.write(self.style.WARNING(f"404  [{status}]: {path}"))
                    results.append([path, status, "NOT_FOUND", "404 Not Found"])
                elif status == 405:
                    self.stdout.write(self.style.NOTICE(f"POST [{status}]: {path} (POST-only action)"))
                    results.append([path, status, "METHOD_NOT_ALLOWED", "405 Method Not Allowed (POST only)"])
                elif 300 <= status < 400:
                    self.stdout.write(self.style.SUCCESS(f"REDIR[{status}]: {path}"))
                    results.append([path, status, "REDIRECT", response.headers.get("Location", "")])
                else:
                    self.stdout.write(self.style.SUCCESS(f"OK   [{status}]: {path}"))
                    results.append([path, status, "OK", ""])

            except Exception as e:
                captured_traceback = memory_handler.get_and_clear()
                err_msg = str(e)
                if is_expected_fail:
                    self.stdout.write(self.style.WARNING(f"EXP-FAIL [EXCEPTION]: {path}"))
                    results.append([path, "EXCEPTION", "EXPECTED_FAIL", err_msg])
                else:
                    self.stdout.write(self.style.ERROR(f"ERROR: {path} -> {err_msg}"))
                    results.append([path, "EXCEPTION", "FAIL", err_msg])
                    error_log_entries.append(
                        f"{'=' * 80}\nURL: {path} [UNCAUGHT EXCEPTION]\n{'=' * 80}\n{captured_traceback or err_msg}\n\n"
                    )

        # 3. Write crawl_errors.log
        if error_log_entries:
            with open(error_log_file, "w") as f:
                f.writelines(error_log_entries)
            self.stdout.write(
                self.style.ERROR(f"\n{len(error_log_entries)} error tracebacks saved to: {error_log_file}")
            )
        else:
            self.stdout.write(self.style.SUCCESS("\nNo errors recorded!"))

        # 4. Export to XLSX
        if openpyxl:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Postgres 17 Crawl Results"

            headers = ["Endpoint URL", "Status Code", "Result", "Details"]
            ws.append(headers)

            header_fill = PatternFill(start_color="1F497D", end_color="1F497D", fill_type="solid")
            header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
            for col_num in range(1, len(headers) + 1):
                cell = ws.cell(row=1, column=col_num)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="left")

            fills = {
                "OK": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
                "REDIRECT": PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid"),
                "METHOD_NOT_ALLOWED": PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid"),
                "FAIL": PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
                "EXPECTED_FAIL": PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"),
                "NOT_FOUND": PatternFill(start_color="FCE4D6", end_color="FCE4D6", fill_type="solid"),
                "SKIPPED": PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid"),
            }

            for row_idx, row_data in enumerate(results, start=2):
                ws.append(row_data)
                result_val = row_data[2]
                if result_val in fills:
                    ws.cell(row=row_idx, column=3).fill = fills[result_val]

            ws.auto_filter.ref = f"A1:D{len(results) + 1}"
            ws.freeze_panes = "A2"

            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                col_letter = openpyxl.utils.get_column_letter(col[0].column)
                ws.column_dimensions[col_letter].width = min(max(max_len + 3, 12), 80)

            wb.save(output_file)
            self.stdout.write(self.style.SUCCESS(f"Spreadsheet saved to: {output_file}"))

        # 5. Print Crawl Summary
        counts = Counter(row[2] for row in results)
        total = len(results)
        fail_count = counts.get("FAIL", 0)

        self.stdout.write("\n" + "=" * 45)
        self.stdout.write(self.style.HTTP_INFO("               CRAWL SUMMARY"))
        self.stdout.write("=" * 45)
        self.stdout.write(f"Total Endpoints Processed : {total}")
        self.stdout.write(self.style.SUCCESS(f"  OK (2xx)               : {counts.get('OK', 0)}"))
        self.stdout.write(self.style.SUCCESS(f"  Redirects (3xx)        : {counts.get('REDIRECT', 0)}"))
        self.stdout.write(self.style.NOTICE(f"  POST-only Actions (405): {counts.get('METHOD_NOT_ALLOWED', 0)}"))
        self.stdout.write(self.style.WARNING(f"  Not Found (404)        : {counts.get('NOT_FOUND', 0)}"))
        self.stdout.write(self.style.WARNING(f"  Expected Failures      : {counts.get('EXPECTED_FAIL', 0)}"))
        self.stdout.write(self.style.WARNING(f"  Skipped                : {counts.get('SKIPPED', 0)}"))

        fail_style = self.style.ERROR if fail_count > 0 else self.style.SUCCESS
        self.stdout.write(fail_style(f"  Failures (5xx / Errors): {fail_count}"))
        self.stdout.write("=" * 45 + "\n")
