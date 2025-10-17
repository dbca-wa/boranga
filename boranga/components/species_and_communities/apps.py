from django.apps import AppConfig


class SpeciesAndCommunitiesConfig(AppConfig):
    name = "boranga.components.species_and_communities"

    def ready(self):
        try:
            from boranga.components.species_and_communities import models as sc_models
            from boranga.utils.uploads import override_upload_to_in_module

            functions_to_wrap = [
                "update_species_doc_filename",
                "update_community_doc_filename",
                "update_species_comms_log_filename",
                "update_community_comms_log_filename",
            ]

            for fname in functions_to_wrap:
                if hasattr(sc_models, fname):
                    override_upload_to_in_module(sc_models, fname)
        except Exception:
            import logging

            logging.getLogger(__name__).exception(
                "Failed to apply randomized upload_to wrappers for species_and_communities"
            )
