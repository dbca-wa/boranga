import logging

from django.conf import settings
from django.contrib.auth.signals import user_logged_in
from django.db import transaction
from ledger_api_client.managed_models import SystemGroup, SystemGroupPermission

from boranga.components.conservation_status.models import (
    ConservationStatusReferral,
    CSExternalRefereeInvite,
)
from boranga.components.occurrence.models import (
    OccurrenceReportReferral,
    OCRExternalRefereeInvite,
)
from boranga.components.users.models import ExternalContributorBlacklist
from boranga.helpers import is_internal, is_internal_contributor

logger = logging.getLogger(__name__)


@transaction.atomic
def ocr_process_ocr_external_referee_invite(sender, user, request, **kwargs):
    """
    Check if the user logging in has an external referee invite and if so, process it.
    """
    logger.info(
        "user_logged_in_signal running ocr_process_external_referee_invite function"
    )
    logger.info("Checking if there are any external referee invites for user: %s", user)
    if not OCRExternalRefereeInvite.objects.filter(
        archived=False, email__iexact=user.email, datetime_first_logged_in__isnull=True
    ).exists():
        return

    logger.info("External occurrence report referee invite found for user: %s", user)

    ocr_external_referee_invites = OCRExternalRefereeInvite.objects.filter(
        email__iexact=user.email, archived=False, datetime_first_logged_in__isnull=True
    )
    if ocr_external_referee_invites.count() > 1:
        logger.warning(
            "Multiple external occurrence report referee invites found for user: %s",
            user,
        )
    ocr_external_referee_invite = ocr_external_referee_invites.first()
    ocr_external_referee_invite.datetime_first_logged_in = user.last_login
    logger.info(
        "Saving datetime_first_logged_in for occurrence report external referee invite: %s",
        ocr_external_referee_invite,
    )
    ocr_external_referee_invite.save()

    OccurrenceReportReferral.objects.create(
        occurrence_report=ocr_external_referee_invite.occurrence_report,
        referral=user.id,
        sent_by=ocr_external_referee_invite.sent_by,
        text=ocr_external_referee_invite.invite_text,
        assigned_officer=ocr_external_referee_invite.sent_by,
        is_external=True,
    )


@transaction.atomic
def cs_process_cs_external_referee_invite(sender, user, request, **kwargs):
    """
    Check if the user logging in has an external referee invite and if so, process it.
    """
    logger.info(
        "user_logged_in_signal running cs_process_external_referee_invite function"
    )
    logger.info("Checking if there are any external referee invites for user: %s", user)
    if not CSExternalRefereeInvite.objects.filter(
        archived=False, email__iexact=user.email, datetime_first_logged_in__isnull=True
    ).exists():
        return

    logger.info("External conservation status referee invite found for user: %s", user)

    cs_external_referee_invites = CSExternalRefereeInvite.objects.filter(
        email__iexact=user.email, archived=False, datetime_first_logged_in__isnull=True
    )
    if cs_external_referee_invites.count() > 1:
        logger.warning(
            "Multiple external conservation status referee invites found for user: %s",
            user,
        )
    cs_external_referee_invite = cs_external_referee_invites.first()
    cs_external_referee_invite.datetime_first_logged_in = user.last_login
    logger.info(
        "Saving datetime_first_logged_in for conservation status external referee invite: %s",
        cs_external_referee_invite,
    )
    cs_external_referee_invite.save()

    ConservationStatusReferral.objects.create(
        conservation_status=cs_external_referee_invite.conservation_status,
        referral=user.id,
        sent_by=cs_external_referee_invite.sent_by,
        text=cs_external_referee_invite.invite_text,
        assigned_officer=cs_external_referee_invite.sent_by,
        is_external=True,
    )


def process_external_referee_invites(sender, user, request, **kwargs):
    ocr_process_ocr_external_referee_invite(sender, user, request, **kwargs)
    cs_process_cs_external_referee_invite(sender, user, request, **kwargs)


def add_external_user_to_external_contributors_group(sender, user, request, **kwargs):
    logger.info(
        "user_logged_in_signal running add_external_user_to_external_contributors_group function"
    )

    external_contributor_group = SystemGroup.objects.get(
        name=settings.GROUP_NAME_EXTERNAL_CONTRIBUTOR
    )

    # Only add external users to the external contributors group
    # The check here has to check if the user is an internal contributor as well because
    # in terms of security checks, the internal contributor group is not included in the INTERNAL_GROUPS
    # in settings.py
    if is_internal(request) or is_internal_contributor(request):
        # Check if the internal user is in the external contributors group and remove them if so
        if SystemGroupPermission.objects.filter(
            system_group=external_contributor_group, emailuser=user
        ).exists():
            SystemGroupPermission.objects.filter(
                system_group=external_contributor_group, emailuser=user
            ).delete()
            external_contributor_group.save()
        return

    # If user is blacklisted, don't add them to the external contributors group
    if ExternalContributorBlacklist.objects.filter(email=user.email).exists():
        logger.info(
            f"User with email {user} has logged in but is blacklisted. Not adding to external contributors group."
        )
        return

    # If user is already in the external contributors group, don't add them again

    if SystemGroupPermission.objects.filter(
        system_group=external_contributor_group, emailuser=user
    ).exists():
        return

    # Add user to the external contributors group
    logger.info(
        f"User with email {user} has logged in for the first time. Adding them to the external contributors group."
    )

    SystemGroupPermission.objects.create(
        system_group=external_contributor_group, emailuser=user
    )
    # Have to save the group to flush the member cache
    external_contributor_group.save()


user_logged_in.connect(add_external_user_to_external_contributors_group)
user_logged_in.connect(process_external_referee_invites)
