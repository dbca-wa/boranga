from django.apps import AppConfig


class ConservationStatusConfig(AppConfig):
    name = "boranga.components.conservation_status"

    def ready(self):
        # Wrap existing upload_to functions to randomize filenames while preserving directories
        try:
            from boranga.components.conservation_status import models as cs_models
            from boranga.utils.uploads import override_upload_to_in_module

            functions_to_wrap = [
                "update_species_conservation_status_comms_log_filename",
                "update_conservation_status_comms_log_filename",
                "update_referral_doc_filename",
                "update_conservation_status_amendment_request_doc_filename",
                "update_conservation_status_doc_filename",
            ]

            for fname in functions_to_wrap:
                if hasattr(cs_models, fname):
                    override_upload_to_in_module(cs_models, fname)
        except Exception:
            # Avoid import-time failures bringing down Django; log if needed
            import logging

            logging.getLogger(__name__).exception(
                "Failed to apply randomized upload_to wrappers for conservation_status"
            )
