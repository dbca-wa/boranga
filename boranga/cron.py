import logging

import django_cron
from django.conf import settings
from django.core import management

log = logging.getLogger(__name__)


class CronJobFetchNomosTaxonDataDaily(django_cron.CronJobBase):
    RUN_ON_DAYS = [0, 1, 2, 3, 4, 5, 6]
    RUN_AT_TIMES = [settings.FETCH_NOMOS_DATA_TIME_OF_DAY]
    schedule = django_cron.Schedule(
        run_weekly_on_days=RUN_ON_DAYS, run_at_times=RUN_AT_TIMES
    )
    code = "boranga.fetch_nomos_data"

    def do(self) -> None:
        log.info("Fetch Nomos Taxon Data cron job triggered, running...")
        management.call_command("fetch_nomos_blob_data")
        return "Job Completed Successfully"


class CronJobOCRPreProcessBulkImportTasks(django_cron.CronJobBase):
    schedule = django_cron.Schedule(
        run_weekly_on_days=[0, 1, 2, 3, 4, 5, 6], run_every_mins=2
    )
    code = "boranga.ocr_pre_process_bulk_import_tasks"

    def do(self) -> None:
        log.info("OCR Pre-process Bulk Import Tasks cron job triggered, running...")
        management.call_command("ocr_pre_process_bulk_import_tasks")
        return "Job Completed Successfully"


class CronJobOCRProcessBulkImportQueue(django_cron.CronJobBase):
    schedule = django_cron.Schedule(
        run_weekly_on_days=[0, 1, 2, 3, 4, 5, 6], run_every_mins=5
    )
    code = "boranga.ocr_process_bulk_import_queue"

    def do(self) -> None:
        log.info("OCR Process Bulk Import Tasks cron job triggered, running...")
        management.call_command("ocr_process_bulk_import_queue")
        return "Job Completed Successfully"


class CronJobAutoLockConservationStatusRecords(django_cron.CronJobBase):
    schedule = django_cron.Schedule(
        run_weekly_on_days=[0, 1, 2, 3, 4, 5, 6], run_every_mins=1
    )
    code = "boranga.auto_lock_conservation_status_records"

    def do(self) -> None:
        management.call_command("auto_lock_unlocked_cs")
        return "Job Completed Successfully"


class CronJobAutoLockUnlockedOccurrenceRecords(django_cron.CronJobBase):
    schedule = django_cron.Schedule(
        run_weekly_on_days=[0, 1, 2, 3, 4, 5, 6], run_every_mins=1
    )
    code = "boranga.auto_lock_unlocked_occurrence_records"

    def do(self) -> None:
        management.call_command("auto_lock_unlocked_occ")
        return "Job Completed Successfully"


class CronJobImportCadastreGeoJSONDaily(django_cron.CronJobBase):
    """Run the import_cadastre_geojson management command once per day at a configured time."""

    RUN_ON_DAYS = [0, 1, 2, 3, 4, 5, 6]
    RUN_AT_TIMES = [settings.IMPORT_CADASTRE_GEOJSON_TIME_OF_DAY]
    schedule = django_cron.Schedule(
        run_weekly_on_days=RUN_ON_DAYS, run_at_times=RUN_AT_TIMES
    )
    code = "boranga.import_cadastre_geojson_daily"

    def do(self) -> None:
        log.info("Import cadastre GeoJSON cron job triggered, running...")
        management.call_command("import_cadastre_geojson")
        return "Job Completed Successfully"
