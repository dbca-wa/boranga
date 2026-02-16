"""
Utilities for cleaning up django-reversion history during data migrations.

This module provides efficient bulk deletion of django-reversion Version records
when clearing target data with `--wipe-targets`, particularly when filtering by
group_type (flora, fauna, community).

Usage:
    from boranga.components.data_migration.utils.reversion_cleanup import ReversionHistoryCleaner

    cleaner = ReversionHistoryCleaner(batch_size=2000)
    cleaner.clear_species_and_related({'group_type__name__in': ['flora']})
    logger.info("Deleted versions: %s", cleaner.get_stats())
"""

import logging
from typing import Any

from django.contrib.contenttypes.models import ContentType
from django.db import models
from reversion.models import Version

logger = logging.getLogger(__name__)


class ReversionHistoryCleaner:
    """
    Clean up django-reversion history for models filtered by group_type.

    This class provides efficient bulk deletion of Version records using batched
    queries to avoid memory issues and query size limits.
    """

    def __init__(self, batch_size: int = 2000):
        """
        Initialize the cleaner.

        Args:
            batch_size: Number of object IDs to process per batch (default: 2000)
        """
        self.batch_size = batch_size
        self.stats = {}

    def clear_for_model(self, model_class: type[models.Model], group_type_filter: dict[str, Any]) -> int:
        """
        Delete Version records for a model filtered by group_type.

        Args:
            model_class: Django model class (e.g., Species)
            group_type_filter: Filter dict, e.g., {'group_type__name__in': ['flora']}
                              Pass empty dict {} to delete all versions for this model.

        Returns:
            Number of versions deleted
        """
        content_type = ContentType.objects.get_for_model(model_class)
        model_name = model_class.__name__

        if not group_type_filter:
            # Unfiltered - get all IDs
            object_ids = list(model_class.objects.values_list("id", flat=True))
        else:
            # Filtered by group_type
            object_ids = list(model_class.objects.filter(**group_type_filter).values_list("id", flat=True))

        if not object_ids:
            logger.info("No %s objects to clean versions for", model_name)
            return 0

        # Convert to strings since Version.object_id is CharField
        str_object_ids = [str(oid) for oid in object_ids]
        total_deleted = 0
        batches = (len(str_object_ids) + self.batch_size - 1) // self.batch_size

        logger.info("Cleaning %d %s versions in %d batch(es)", len(object_ids), model_name, batches)

        for i in range(0, len(str_object_ids), self.batch_size):
            batch_ids = str_object_ids[i : i + self.batch_size]
            deleted_count, _ = Version.objects.filter(content_type=content_type, object_id__in=batch_ids).delete()

            total_deleted += deleted_count

            if deleted_count > 0:
                logger.debug(
                    "Deleted %d %s versions (batch %d/%d)", deleted_count, model_name, i // self.batch_size + 1, batches
                )

        self.stats[model_name] = total_deleted
        if total_deleted > 0:
            logger.info("Deleted %d %s versions total", total_deleted, model_name)
        return total_deleted

    def clear_for_related_model(
        self, model_class: type[models.Model], related_field_path: str, group_type_filter: dict[str, Any]
    ) -> int:
        """
        Delete versions for a model related to a group_type model via FK.

        Example: Delete Taxonomy versions where taxonomy.species.group_type = 'flora'

        Args:
            model_class: Model to clean (e.g., Taxonomy)
            related_field_path: Path to parent model (e.g., 'species' or 'taxonomy__species')
            group_type_filter: Filter for the related model, e.g., {'group_type__name__in': ['flora']}

        Returns:
            Number of versions deleted
        """
        content_type = ContentType.objects.get_for_model(model_class)
        model_name = model_class.__name__

        if not group_type_filter:
            object_ids = list(model_class.objects.values_list("id", flat=True))
        else:
            # Construct the filter path
            filter_kwargs = {}
            for key, value in group_type_filter.items():
                # Build filter like 'species__group_type__name__in' or 'taxonomy__species__group_type__name__in'
                full_path = f"{related_field_path}__{key}" if related_field_path else key
                filter_kwargs[full_path] = value

            object_ids = list(model_class.objects.filter(**filter_kwargs).values_list("id", flat=True))

        if not object_ids:
            logger.info("No %s objects to clean versions for", model_name)
            return 0

        str_object_ids = [str(oid) for oid in object_ids]
        total_deleted = 0
        batches = (len(str_object_ids) + self.batch_size - 1) // self.batch_size

        logger.info("Cleaning %d %s versions in %d batch(es)", len(object_ids), model_name, batches)

        for i in range(0, len(str_object_ids), self.batch_size):
            batch_ids = str_object_ids[i : i + self.batch_size]
            deleted_count, _ = Version.objects.filter(content_type=content_type, object_id__in=batch_ids).delete()
            total_deleted += deleted_count

        self.stats[model_name] = total_deleted
        if total_deleted > 0:
            logger.info("Deleted %d %s versions total", total_deleted, model_name)
        return total_deleted

    def clear_species_and_related(self, group_type_filter: dict[str, Any]) -> int:
        """
        Delete all Species and related model versions for group_type filter.

        Args:
            group_type_filter: e.g., {'group_type__name__in': ['flora']} or {} for all

        Returns:
            Total versions deleted across all models
        """
        from boranga.components.species_and_communities.models import (
            Species,
            SpeciesConservationAttributes,
            SpeciesDistribution,
            SpeciesDocument,
            SpeciesPublishingStatus,
            Taxonomy,
            TaxonPreviousName,
            TaxonVernacular,
        )

        logger.info("=== Clearing Species reversion history ===")

        # Main model first
        self.clear_for_model(Species, group_type_filter)

        # Related models (most are direct FK to Species, some through Taxonomy)
        models_to_clear = [
            (SpeciesDocument, "species"),
            (SpeciesDistribution, "species"),
            (SpeciesConservationAttributes, "species"),
            (SpeciesPublishingStatus, "species"),
            (Taxonomy, "species_set"),  # Reverse FK
        ]

        for model_class, related_path in models_to_clear:
            try:
                self.clear_for_related_model(model_class, related_path, group_type_filter)
            except Exception as e:
                logger.warning("Failed to clear %s versions: %s", model_class.__name__, e)

        # Handle Taxonomy-related models (nested relationship)
        taxonomy_models = [
            (TaxonPreviousName, "taxonomy__species_set"),
            (TaxonVernacular, "taxonomy__species_set"),
        ]

        for model_class, related_path in taxonomy_models:
            try:
                self.clear_for_related_model(model_class, related_path, group_type_filter)
            except Exception as e:
                logger.warning("Failed to clear %s versions: %s", model_class.__name__, e)

        total = sum(self.stats.values())
        logger.info("=== Total Species-related versions deleted: %d ===", total)
        return total

    def clear_community_and_related(self, group_type_filter: dict[str, Any]) -> int:
        """
        Delete all Community and related model versions.

        Args:
            group_type_filter: e.g., {'group_type__name__in': ['community']} or {} for all

        Returns:
            Total versions deleted across all models
        """
        from boranga.components.species_and_communities.models import (
            Community,
            CommunityConservationAttributes,
            CommunityDistribution,
            CommunityDocument,
            CommunityPublishingStatus,
            CommunityTaxonomy,
        )

        logger.info("=== Clearing Community reversion history ===")

        self.clear_for_model(Community, group_type_filter)

        models_to_clear = [
            (CommunityDocument, "community"),
            (CommunityTaxonomy, "community"),
            (CommunityDistribution, "community"),
            (CommunityConservationAttributes, "community"),
            (CommunityPublishingStatus, "community"),
        ]

        for model_class, related_path in models_to_clear:
            try:
                self.clear_for_related_model(model_class, related_path, group_type_filter)
            except Exception as e:
                logger.warning("Failed to clear %s versions: %s", model_class.__name__, e)

        total = sum(self.stats.values())
        logger.info("=== Total Community-related versions deleted: %d ===", total)
        return total

    def clear_occurrence_and_related(self, group_type_filter: dict[str, Any]) -> int:
        """
        Delete all Occurrence and OccurrenceReport versions with related models.

        Args:
            group_type_filter: e.g., {'group_type__name__in': ['community']} or {} for all

        Returns:
            Total versions deleted across all models
        """
        from boranga.components.occurrence.models import (
            OCCAssociatedSpecies,
            OCCConservationThreat,
            OCCContactDetail,
            OCCFireHistory,
            OCCHabitatComposition,
            OCCHabitatCondition,
            OCCIdentification,
            OCCLocation,
            OCCObservationDetail,
            Occurrence,
            OccurrenceDocument,
            OccurrenceGeometry,
            OccurrenceReport,
            OccurrenceReportDocument,
            OccurrenceReportGeometry,
            OccurrenceSite,
            OCCVegetationStructure,
            OCRAnimalObservation,
            OCRAssociatedSpecies,
            OCRConservationThreat,
            OCRFireHistory,
            OCRHabitatComposition,
            OCRHabitatCondition,
            OCRIdentification,
            OCRObservationDetail,
            OCRObserverDetail,
            OCRPlantCount,
            OCRVegetationStructure,
        )

        logger.info("=== Clearing Occurrence reversion history ===")

        # Main models with group_type
        self.clear_for_model(Occurrence, group_type_filter)
        self.clear_for_model(OccurrenceReport, group_type_filter)

        # Occurrence-related models (OCC*)
        occ_models = [
            (OccurrenceGeometry, "occurrence"),
            (OCCContactDetail, "occurrence"),
            (OCCLocation, "occurrence"),
            (OccurrenceSite, "occurrence"),
            (OCCObservationDetail, "occurrence"),
            (OCCHabitatComposition, "occurrence"),
            (OCCFireHistory, "occurrence"),
            (OCCAssociatedSpecies, "occurrence"),
            (OccurrenceDocument, "occurrence"),
            (OCCIdentification, "occurrence"),
            (OCCHabitatCondition, "occurrence"),
            (OCCVegetationStructure, "occurrence"),
            (OCCConservationThreat, "occurrence"),
        ]

        for model_class, related_path in occ_models:
            try:
                self.clear_for_related_model(model_class, related_path, group_type_filter)
            except Exception as e:
                logger.warning("Failed to clear %s versions: %s", model_class.__name__, e)

        # OccurrenceReport-related models (OCR*)
        ocr_models = [
            (OccurrenceReportGeometry, "occurrence_report"),
            (OCRObserverDetail, "occurrence_report"),
            (OCRHabitatComposition, "occurrence_report"),
            (OCRHabitatCondition, "occurrence_report"),
            (OCRVegetationStructure, "occurrence_report"),
            (OCRFireHistory, "occurrence_report"),
            (OCRAssociatedSpecies, "occurrence_report"),
            (OCRObservationDetail, "occurrence_report"),
            (OCRPlantCount, "occurrence_report"),
            (OCRAnimalObservation, "occurrence_report"),
            (OCRIdentification, "occurrence_report"),
            (OccurrenceReportDocument, "occurrence_report"),
            (OCRConservationThreat, "occurrence_report"),
        ]

        for model_class, related_path in ocr_models:
            try:
                self.clear_for_related_model(model_class, related_path, group_type_filter)
            except Exception as e:
                logger.warning("Failed to clear %s versions: %s", model_class.__name__, e)

        total = sum(self.stats.values())
        logger.info("=== Total Occurrence-related versions deleted: %d ===", total)
        return total

    def get_stats(self) -> dict[str, int]:
        """
        Return deletion statistics.

        Returns:
            Dictionary mapping model name to number of versions deleted
        """
        return self.stats.copy()
