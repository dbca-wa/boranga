import logging
from datetime import timedelta

from django.core.exceptions import ValidationError
from django.db import transaction
from django.http import HttpRequest
from django.utils import timezone

from boranga.components.conservation_status.models import (
    ConservationStatus,
    ConservationStatusUserAction,
)
from boranga.components.species_and_communities.email import (
    send_community_create_email_notification,
    send_species_create_email_notification,
    send_species_rename_email_notification,
    send_user_community_create_email_notification,
    send_user_species_create_email_notification,
)
from boranga.components.species_and_communities.models import (
    Community,
    CommunityUserAction,
    ConservationThreat,
    District,
    Region,
    Species,
    SpeciesConservationAttributes,
    SpeciesDistribution,
    SpeciesDocument,
    SpeciesUserAction,
)

logger = logging.getLogger(__name__)


@transaction.atomic
def species_form_submit(species_instance, request, split=False, rename=False):
    if not split and not rename:
        if not species_instance.can_user_edit:
            raise ValidationError("You can't submit this species at this moment")

    species_instance.submitter = request.user.id
    species_instance.lodgement_date = timezone.now()

    # Create a log entry for the proposal
    species_instance.log_user_action(
        SpeciesUserAction.ACTION_CREATE_SPECIES.format(species_instance.species_number),
        request,
    )

    # Create a log entry for the user
    request.user.log_user_action(
        SpeciesUserAction.ACTION_CREATE_SPECIES.format(species_instance.species_number),
        request,
    )

    ret1 = send_species_create_email_notification(request, species_instance)
    ret2 = send_user_species_create_email_notification(request, species_instance)

    if ret1 and ret2:
        species_instance.processing_status = Species.PROCESSING_STATUS_ACTIVE
        # all functions that call this save after - otherwise we can parametise this if need be
        species_instance.save(no_revision=True)
    else:
        raise ValidationError(
            "An error occurred while submitting proposal (Submit email notifications failed)"
        )
    return species_instance


@transaction.atomic
def community_form_submit(community_instance, request):
    if not community_instance.can_user_edit:
        raise ValidationError("You can't submit this community at this moment")

    community_instance.submitter = request.user.id
    community_instance.lodgement_date = timezone.now()

    # Create a log entry for the proposal
    community_instance.log_user_action(
        CommunityUserAction.ACTION_CREATE_COMMUNITY.format(
            community_instance.community_number
        ),
        request,
    )

    # Create a log entry for the user
    request.user.log_user_action(
        CommunityUserAction.ACTION_CREATE_COMMUNITY.format(
            community_instance.community_number
        ),
        request,
    )

    ret1 = send_community_create_email_notification(request, community_instance)
    ret2 = send_user_community_create_email_notification(request, community_instance)

    if ret1 and ret2:
        community_instance.processing_status = Community.PROCESSING_STATUS_ACTIVE
        community_instance.save(no_revision=True)
    else:
        raise ValidationError(
            "An error occurred while submitting proposal (Submit email notifications failed)"
        )

    return community_instance


def update_related_occurrence_reports(
    original_species, new_species, request, action_type="renamed"
):
    from boranga.components.occurrence.email import (
        send_occurrence_report_species_renamed_email,
    )
    from boranga.components.occurrence.models import OccurrenceReport

    related_orfs = OccurrenceReport.objects.filter(species=original_species).exclude(
        processing_status=OccurrenceReport.PROCESSING_STATUS_APPROVED
    )

    for orf in related_orfs:
        orf.species = new_species
        orf.save()

        # Log action
        action_msg = "Species name was automatically changed because the old name {} was {} to {}.".format(
            original_species.species_number,
            action_type,
            new_species.species_number,
        )
        orf.log_user_action(action_msg, request)

        # Send email
        send_occurrence_report_species_renamed_email(
            request, orf, original_species, new_species
        )


@transaction.atomic
def process_species_from_combine_list(
    species_instance,
    resulting_species_instance,
    resulting_species_exists,
    actions,
    request,
):
    if species_instance == resulting_species_instance:
        # The resulting species doesn't have it's processing status changed
        # And it's action logs are done in the api endpoint
        return

    if species_instance.processing_status not in [
        Species.PROCESSING_STATUS_ACTIVE,
        Species.PROCESSING_STATUS_DRAFT,
        Species.PROCESSING_STATUS_HISTORICAL,
    ]:
        raise ValidationError(
            "Species {} has a processing status of {} so can not be part "
            "of a combine action (must be one of {})".format(
                species_instance.species_number,
                species_instance.processing_status,
                [
                    Species.PROCESSING_STATUS_ACTIVE,
                    Species.PROCESSING_STATUS_DRAFT,
                    Species.PROCESSING_STATUS_HISTORICAL,
                ],
            )
        )

    if species_instance.processing_status == Species.PROCESSING_STATUS_ACTIVE:
        species_instance.processing_status = Species.PROCESSING_STATUS_HISTORICAL
        species_instance.save(version_user=request.user)

        # If there is an approved conservation status for this species, close it
        active_conservation_status = ConservationStatus.objects.filter(
            species=species_instance,
            processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED,
        ).first()
        if active_conservation_status:
            # effective_to date should be populated based on the Resultant Profile's Approved CS effective_from date
            # i.e. Original Closed CS effective_to = Resultant Approved CS effective_from minus one day.
            resulting_cs = ConservationStatus.objects.filter(
                species=resulting_species_instance,
                processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED,
            ).first()
            if resulting_cs and resulting_cs.effective_from:
                active_conservation_status.effective_to = (
                    resulting_cs.effective_from - timedelta(days=1)
                )

            active_conservation_status.customer_status = (
                ConservationStatus.CUSTOMER_STATUS_CLOSED
            )
            active_conservation_status.processing_status = (
                ConservationStatus.PROCESSING_STATUS_CLOSED
            )
            active_conservation_status.save(version_user=request.user)

            active_conservation_status.log_user_action(
                ConservationStatusUserAction.ACTION_CLOSE_CONSERVATION_STATUS_DUE_TO_COMBINE.format(
                    active_conservation_status.conservation_status_number
                ),
                request,
            )
            request.user.log_user_action(
                ConservationStatusUserAction.ACTION_CLOSE_CONSERVATION_STATUS_DUE_TO_COMBINE.format(
                    active_conservation_status.conservation_status_number
                ),
                request,
            )

        ACTION = SpeciesUserAction.ACTION_COMBINE_ACTIVE_SPECIES_TO_NEW
        if resulting_species_exists:
            ACTION = SpeciesUserAction.ACTION_COMBINE_ACTIVE_SPECIES_TO_EXISTING

        species_instance.log_user_action(
            ACTION.format(
                species_instance.species_number,
                resulting_species_instance.species_number,
            ),
            request,
        )
        request.user.log_user_action(
            ACTION.format(
                species_instance.species_number,
                resulting_species_instance.species_number,
            ),
            request,
        )
        actions[species_instance.id] = Species.COMBINE_SPECIES_ACTION_MADE_HISTORICAL
    elif species_instance.processing_status == Species.PROCESSING_STATUS_DRAFT:
        species_instance.processing_status = Species.PROCESSING_STATUS_DISCARDED
        species_instance.save(version_user=request.user)

        ACTION = SpeciesUserAction.ACTION_COMBINE_DRAFT_SPECIES_TO_NEW
        if resulting_species_exists:
            ACTION = SpeciesUserAction.ACTION_COMBINE_DRAFT_SPECIES_TO_EXISTING

        species_instance.log_user_action(
            ACTION.format(
                species_instance.species_number,
                resulting_species_instance.species_number,
            ),
            request,
        )
        request.user.log_user_action(
            ACTION.format(
                species_instance.species_number,
                resulting_species_instance.species_number,
            ),
            request,
        )
        actions[species_instance.id] = Species.COMBINE_SPECIES_ACTION_DISCARDED

    elif species_instance.processing_status == Species.PROCESSING_STATUS_HISTORICAL:
        ACTION = SpeciesUserAction.ACTION_COMBINE_HISTORICAL_SPECIES_TO_NEW
        if resulting_species_exists:
            ACTION = SpeciesUserAction.ACTION_COMBINE_HISTORICAL_SPECIES_TO_EXISTING

        species_instance.log_user_action(
            ACTION.format(
                species_instance.species_number,
                resulting_species_instance.species_number,
            ),
            request,
        )
        request.user.log_user_action(
            ACTION.format(
                species_instance.species_number,
                resulting_species_instance.species_number,
            ),
            request,
        )
        actions[species_instance.id] = Species.COMBINE_SPECIES_ACTION_LEFT_AS_HISTORICAL

    update_related_occurrence_reports(
        species_instance, resulting_species_instance, request, "combined"
    )

    return species_instance


@transaction.atomic
def rename_species_original_submit(species_instance, new_species, request):
    if species_instance.processing_status != Species.PROCESSING_STATUS_ACTIVE:
        raise ValidationError("You can't submit this species at this moment")

    species_instance.processing_status = Species.PROCESSING_STATUS_HISTORICAL
    species_instance.save(version_user=request.user)

    # If there is an approved conservation status for this species, close it
    active_conservation_status = ConservationStatus.objects.filter(
        species=species_instance,
        processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED,
    ).first()
    if active_conservation_status:
        # effective_to date should be populated based on the Resultant Profile's Approved CS effective_from date
        # i.e. Original Closed CS effective_to = Resultant Approved CS effective_from minus one day.
        new_species_cs = ConservationStatus.objects.filter(
            species=new_species,
            processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED,
        ).first()
        if new_species_cs and new_species_cs.effective_from:
            active_conservation_status.effective_to = (
                new_species_cs.effective_from - timedelta(days=1)
            )

        active_conservation_status.customer_status = (
            ConservationStatus.CUSTOMER_STATUS_CLOSED
        )
        active_conservation_status.processing_status = (
            ConservationStatus.PROCESSING_STATUS_CLOSED
        )
        active_conservation_status.save(version_user=request.user)

        active_conservation_status.log_user_action(
            ConservationStatusUserAction.ACTION_CLOSE_CONSERVATION_STATUS_DUE_TO_RENAME.format(
                active_conservation_status.conservation_status_number
            ),
            request,
        )
        request.user.log_user_action(
            ConservationStatusUserAction.ACTION_CLOSE_CONSERVATION_STATUS_DUE_TO_RENAME.format(
                active_conservation_status.conservation_status_number
            ),
            request,
        )

    # send the rename species email notification
    send_species_rename_email_notification(request, species_instance, new_species)

    update_related_occurrence_reports(species_instance, new_species, request, "renamed")

    return species_instance


@transaction.atomic
def rename_deep_copy(
    request: HttpRequest, instance: Species, existing_species: Species = None
) -> Species:
    # related items to instance that needs to create for new rename instance as well
    instance_documents = SpeciesDocument.objects.filter(species=instance.id)
    instance_threats = ConservationThreat.objects.filter(species=instance.id)
    src_cons_attrs = SpeciesConservationAttributes.objects.filter(
        species=instance.id
    ).first()
    src_distribution = SpeciesDistribution.objects.filter(species=instance.id).first()

    if existing_species:
        resulting_species = existing_species
        resulting_species.department_file_numbers = instance.department_file_numbers
        resulting_species.last_data_curation_date = instance.last_data_curation_date
        resulting_species.conservation_plan_exists = instance.conservation_plan_exists
        resulting_species.conservation_plan_reference = (
            instance.conservation_plan_reference
        )
        resulting_species.comment = instance.comment
    else:
        # clone the species instance into new rename instance
        resulting_species = Species.objects.get(pk=instance.pk)
        resulting_species.id = None
        resulting_species.taxonomy_id = None
        resulting_species.species_number = ""
        resulting_species.lodgement_date = None

    resulting_species.save(version_user=request.user)

    # Copy the regions an districts
    resulting_species.regions.set(instance.regions.all())
    resulting_species.districts.set(instance.districts.all())

    # Log action
    resulting_species.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_FROM.format(
            resulting_species.species_number,
            instance.species_number,
        ),
        request,
    )
    request.user.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_FROM.format(
            resulting_species.species_number,
            instance.species_number,
        ),
        request,
    )
    instance.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_TO.format(
            instance.species_number,
            resulting_species.species_number,
        ),
        request,
    )
    request.user.log_user_action(
        SpeciesUserAction.ACTION_COPY_SPECIES_TO.format(
            instance.species_number,
            resulting_species.species_number,
        ),
        request,
    )

    for new_document in instance_documents:
        new_doc_instance = new_document
        new_doc_instance.species = resulting_species
        new_doc_instance.id = None
        new_doc_instance.document_number = ""
        new_doc_instance.save(version_user=request.user)
        new_doc_instance.species.log_user_action(
            SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                new_doc_instance.document_number,
                new_doc_instance.species.species_number,
            ),
            request,
        )
        request.user.log_user_action(
            SpeciesUserAction.ACTION_ADD_DOCUMENT.format(
                new_doc_instance.document_number,
                new_doc_instance.species.species_number,
            ),
            request,
        )

    for new_threat in instance_threats:
        new_threat_instance = new_threat
        new_threat_instance.species = resulting_species
        new_threat_instance.id = None
        new_threat_instance.threat_number = ""
        new_threat_instance.save(version_user=request.user)
        new_threat_instance.species.log_user_action(
            SpeciesUserAction.ACTION_ADD_THREAT.format(
                new_threat_instance.threat_number,
                new_threat_instance.species.species_number,
            ),
            request,
        )
        request.user.log_user_action(
            SpeciesUserAction.ACTION_ADD_THREAT.format(
                new_threat_instance.threat_number,
                new_threat_instance.species.species_number,
            ),
            request,
        )

    # Conservation Attributes (OneToOne): update-or-create, no duplicates
    if src_cons_attrs:
        dest_cons_attrs, _ = SpeciesConservationAttributes.objects.get_or_create(
            species=resulting_species
        )
        for f in dest_cons_attrs._meta.concrete_fields:
            if f.name in ("id", "species"):
                continue
            setattr(dest_cons_attrs, f.name, getattr(src_cons_attrs, f.name))
        dest_cons_attrs.save()

    # Distribution (OneToOne): update-or-create, no duplicates
    if src_distribution:
        dest_distribution, _ = SpeciesDistribution.objects.get_or_create(
            species=resulting_species
        )
        for f in dest_distribution._meta.concrete_fields:
            if f.name in ("id", "species"):
                continue
            setattr(dest_distribution, f.name, getattr(src_distribution, f.name))
        dest_distribution.save()

    return resulting_species


def process_species_general_data(species_instance, species_request_data):
    species_instance.department_file_numbers = species_request_data.get(
        "department_file_numbers", None
    )
    species_instance.last_data_curation_date = species_request_data.get(
        "last_data_curation_date", None
    )
    species_instance.conservation_plan_exists = (
        species_request_data.get("conservation_plan_exists", False) == "true"
    )
    species_instance.comment = species_request_data.get("comment", None)


def process_species_regions_and_districts(species_instance, species_request_data):
    regions_ids = species_request_data.get("regions", [])
    districts_ids = species_request_data.get("districts", [])

    if not regions_ids and not districts_ids:
        raise ValidationError(
            "At least one region or district must be provided for split species with taxonomy id: {}.".format(
                species_instance.taxonomy_id
            )
        )

    regions = Region.objects.filter(id__in=regions_ids)
    districts = District.objects.filter(id__in=districts_ids)

    species_instance.regions.set(regions)
    species_instance.districts.set(districts)


def process_species_distribution_data(species_instance, species_request_data):
    distribution_request_data = species_request_data.get("distribution", None)
    if not distribution_request_data:
        raise ValidationError(
            "Distribution data is required for split species with taxonomy id: {}.".format(
                species_instance.taxonomy_id
            )
        )

    distribution_instance, created = SpeciesDistribution.objects.get_or_create(
        species=species_instance
    )

    distribution_instance.number_of_occurrences = distribution_request_data.get(
        "number_of_occurrences", None
    )
    distribution_instance.noo_auto = distribution_request_data.get("noo_auto", None)
    distribution_instance.extent_of_occurrences = distribution_request_data.get(
        "extent_of_occurrences", None
    )
    distribution_instance.eoo_auto = distribution_request_data.get("eoo_auto", None)
    distribution_instance.area_of_occupancy = distribution_request_data.get(
        "area_of_occupancy", None
    )
    distribution_instance.area_of_occupancy_actual = distribution_request_data.get(
        "area_of_occupancy_actual", None
    )
    distribution_instance.aoo_actual_auto = distribution_request_data.get(
        "aoo_actual_auto", None
    )
    distribution_instance.number_of_iucn_locations = distribution_request_data.get(
        "number_of_iucn_locations", None
    )
    distribution_instance.number_of_iucn_subpopulations = distribution_request_data.get(
        "number_of_iucn_subpopulations", None
    )
    distribution_instance.iucn_locations_auto = distribution_request_data.get(
        "iucn_locations_auto", None
    )
    distribution_instance.distribution = distribution_request_data.get(
        "distribution", None
    )
    distribution_instance.save()
