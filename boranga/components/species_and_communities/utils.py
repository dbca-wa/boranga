import logging

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

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
    Species,
    SpeciesUserAction,
)

logger = logging.getLogger(__name__)


@transaction.atomic
def species_form_submit(species_instance, request, split=False):
    if split:
        if not species_instance.can_user_split:
            raise ValidationError("You can't split this species at this moment")
    else:
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

    if (settings.WORKING_FROM_HOME and settings.DEBUG) or ret1 and ret2:
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

    if (settings.WORKING_FROM_HOME and settings.DEBUG) or ret1 and ret2:
        community_instance.processing_status = Community.PROCESSING_STATUS_ACTIVE
        community_instance.save(no_revision=True)
    else:
        raise ValidationError(
            "An error occurred while submitting proposal (Submit email notifications failed)"
        )

    return community_instance


@transaction.atomic
def combine_species_original_submit(species_instance, request):
    if species_instance.processing_status != Species.PROCESSING_STATUS_ACTIVE:
        raise ValidationError("You can't submit this species at this moment")

    species_instance.processing_status = Species.PROCESSING_STATUS_HISTORICAL
    species_instance.save(version_user=request.user)

    return species_instance


@transaction.atomic
def rename_species_original_submit(species_instance, new_species, request):
    if species_instance.processing_status != Species.PROCESSING_STATUS_ACTIVE:
        raise ValidationError("You can't submit this species at this moment")

    species_instance.processing_status = Species.PROCESSING_STATUS_HISTORICAL
    species_instance.save(version_user=request.user)

    # send the rename species email notification
    send_species_rename_email_notification(request, species_instance, new_species)

    return species_instance
