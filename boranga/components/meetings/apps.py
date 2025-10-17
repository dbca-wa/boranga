from django.apps import AppConfig


class MeetingsConfig(AppConfig):
    name = "boranga.components.meetings"

    def ready(self):
        try:
            from boranga.components.meetings import models as meet_models
            from boranga.utils.uploads import override_upload_to_in_module

            functions_to_wrap = [
                "update_meeting_comms_log_filename",
                "update_meeting_doc_filename",
            ]

            for fname in functions_to_wrap:
                if hasattr(meet_models, fname):
                    override_upload_to_in_module(meet_models, fname)
        except Exception:
            import logging

            logging.getLogger(__name__).exception(
                "Failed to apply randomized upload_to wrappers for meetings"
            )
