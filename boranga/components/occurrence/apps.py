from django.apps import AppConfig


class OccurrenceConfig(AppConfig):
    name = "boranga.components.occurrence"

    def ready(self):
        try:
            from boranga.components.occurrence import models as occ_models
            from boranga.utils.uploads import override_upload_to_in_module

            functions_to_wrap = [
                "update_occurrence_report_comms_log_filename",
                "update_occurrence_report_doc_filename",
                "update_occurrence_doc_filename",
                "update_occurrence_report_referral_doc_filename",
                "update_occurrence_report_amendment_request_doc_filename",
                "update_occurrence_comms_log_filename",
                # NOTE: update_occurrence_report_shapefile_doc_filename is NOT wrapped
                # because it needs to preserve base filenames for shapefile components
            ]

            for fname in functions_to_wrap:
                if hasattr(occ_models, fname):
                    override_upload_to_in_module(occ_models, fname)
        except Exception:
            import logging

            logging.getLogger(__name__).exception("Failed to apply randomized upload_to wrappers for occurrence")
