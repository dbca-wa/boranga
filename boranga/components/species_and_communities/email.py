import logging

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.mail import EmailMessage, EmailMultiAlternatives
from django.urls import reverse
from django.utils.encoding import smart_str
from ledger_api_client.ledger_models import EmailUserRO as EmailUser

from boranga.components.conservation_status.models import ConservationStatus
from boranga.components.emails.emails import TemplateEmailBase
from boranga.components.occurrence.models import Occurrence
from boranga.components.species_and_communities.models import (
    SystemEmailGroup,
    private_storage,
)
from boranga.helpers import convert_external_url_to_internal_url

logger = logging.getLogger(__name__)

SYSTEM_NAME = settings.SYSTEM_NAME_SHORT + " Automated Message"


def get_sender_user():
    sender = settings.DEFAULT_FROM_EMAIL
    try:
        sender_user = EmailUser.objects.get(email__icontains=sender)
    except EmailUser.DoesNotExist:
        EmailUser.objects.create(email=sender, password="")
        sender_user = EmailUser.objects.get(email__icontains=sender)
    return sender_user


class CreateSpeciesSendNotificationEmail(TemplateEmailBase):
    subject = "A new Species has been created."
    html_template = "boranga/emails/send_create_notification.html"
    txt_template = "boranga/emails/send_create_notification.txt"


class UserCreateSpeciesSendNotificationEmail(TemplateEmailBase):
    subject = f"{settings.DEP_NAME} - Confirmation - Species submitted."
    html_template = "boranga/emails/send_user_create_notification.html"
    txt_template = "boranga/emails/send_user_create_notification.txt"


class SplitSpeciesSendNotificationEmail(TemplateEmailBase):
    subject = "A Species has been split."
    html_template = "boranga/emails/send_split_notification.html"
    txt_template = "boranga/emails/send_split_notification.txt"


class CombineSpeciesSendNotificationEmail(TemplateEmailBase):
    subject = "The Species has been combined."
    html_template = "boranga/emails/send_combine_notification.html"
    txt_template = "boranga/emails/send_combine_notification.txt"


class RenameSpeciesSendNotificationEmail(TemplateEmailBase):
    subject = "A Species has been renamed."
    html_template = "boranga/emails/send_rename_notification.html"
    txt_template = "boranga/emails/send_rename_notification.txt"


class RenameCommunitySendNotificationEmail(TemplateEmailBase):
    subject = "A Community has been renamed."
    html_template = "boranga/emails/send_rename_community_notification.html"
    txt_template = "boranga/emails/send_rename_community_notification.txt"


class CreateCommunitySendNotificationEmail(TemplateEmailBase):
    subject = "A new Community has been created."
    html_template = "boranga/emails/send_create_notification.html"
    txt_template = "boranga/emails/send_create_notification.txt"


class UserCreateCommunitySendNotificationEmail(TemplateEmailBase):
    subject = f"{settings.DEP_NAME} - Confirmation - Community submitted."
    html_template = "boranga/emails/send_user_create_notification.html"
    txt_template = "boranga/emails/send_user_create_notification.txt"


class NomosScriptFailedEmail(TemplateEmailBase):
    subject = "Failed: NOMOS API Management Script"
    html_template = "boranga/emails/send_nomos_api_failed_notification.html"
    txt_template = "boranga/emails/send_nomos_api_failed_notification.txt"


def send_species_create_email_notification(request, species_proposal):
    email = CreateSpeciesSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-species-detail",
            kwargs={"species_proposal_pk": species_proposal.id},
        )
    )
    # add the extra query params as need to load the species detail page
    url = url + f"?group_type_name={species_proposal.group_type.name}&action=view"
    url = convert_external_url_to_internal_url(url)

    context = {"species_community_proposal": species_proposal, "url": url}

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=species_proposal.group_type,
    )

    msg = email.send(recipients, context=context)
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_species_email(msg, species_proposal, sender=sender)

    return msg


# created email send to the user who is internal not external
def send_user_species_create_email_notification(request, species_proposal):
    email = UserCreateSpeciesSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-species-detail",
            kwargs={"species_proposal_pk": species_proposal.id},
        )
    )
    # add the extra query params as need to load the species detail page
    url = url + f"?group_type_name={species_proposal.group_type.name}&action=view"

    url = convert_external_url_to_internal_url(url)

    if species_proposal.submitter:
        submitter_name = EmailUser.objects.get(id=species_proposal.submitter).get_full_name()
    else:
        submitter_name = "Unknown"

    context = {
        "species_community_proposal": species_proposal,
        "submitter": submitter_name,
        "url": url,
    }
    all_ccs = []

    msg = email.send(
        request.user.email,
        cc=all_ccs,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_species_email(msg, species_proposal, sender=sender)

    return msg


# here species_proposal is the original species from split functionality
def send_species_split_email_notification(
    request,
    original_species,
    split_species_list,
    split_of_species_retains_original,
    occurrence_assignments_dict,
):
    email = SplitSpeciesSendNotificationEmail()
    url = request.build_absolute_uri(reverse("internal-conservation-status-dashboard", kwargs={}))
    url = convert_external_url_to_internal_url(url)

    notification_emails = SystemEmailGroup.emails_by_group_and_area(
        group_type=original_species.group_type,
    )

    all_ccs = notification_emails

    conservation_status_url = []
    conservation_status = original_species.conservation_status.filter(
        processing_status=ConservationStatus.PROCESSING_STATUS_APPROVED
    ).first()
    if conservation_status:
        conservation_status_url = request.build_absolute_uri(
            reverse(
                "internal-conservation-status-detail",
                kwargs={"cs_proposal_pk": conservation_status.id},
            )
        )
        cs_notification_emails = SystemEmailGroup.emails_by_group_and_area(
            group_type=original_species.group_type,
            area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
        )
        all_ccs.extend(cs_notification_emails)

    occurrences = {}
    if len(occurrence_assignments_dict.keys()) > 0:
        occurrence_qs = Occurrence.objects.filter(
            id__in=occurrence_assignments_dict.keys(),
        )
        for occurrence in occurrence_qs:
            species_key = f"{occurrence.species.species_number} - {occurrence.species.taxonomy.scientific_name}"
            occ_dict = {
                "occurrence_number": occurrence.occurrence_number,
                "occurrence_name": occurrence.occurrence_name,
                "processing_status": occurrence.processing_status,
            }
            if species_key not in occurrences:
                occurrences[species_key] = []
            occurrences[species_key].append(occ_dict)

        occ_notification_emails = SystemEmailGroup.emails_by_group_and_area(
            group_type=original_species.group_type,
            area=SystemEmailGroup.AREA_OCCURRENCE,
        )
        all_ccs.extend(occ_notification_emails)

    context = {
        "original_species": original_species,
        "split_species_list": split_species_list,
        "url": url,
        "conservation_status_url": conservation_status_url,
        "occurrences": occurrences,
        "split_of_species_retains_original": split_of_species_retains_original,
    }

    all_ccs = list(set(all_ccs))

    msg = email.send(
        request.user.email,
        cc=all_ccs,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_species_email(msg, original_species, sender=sender)

    return msg


#  here species_proposal is the new species created in combine species functionality
def send_species_combine_email_notification(request, combine_species_qs, resulting_species_instance, actions):
    email = CombineSpeciesSendNotificationEmail()

    url = request.build_absolute_uri(reverse("internal-conservation-status-dashboard", kwargs={}))
    url = convert_external_url_to_internal_url(url)
    resulting_species_url = request.build_absolute_uri(
        reverse(
            "internal-species-detail",
            kwargs={"species_proposal_pk": resulting_species_instance.id},
        )
    )

    notification_emails = SystemEmailGroup.emails_by_group_and_area(
        group_type=resulting_species_instance.group_type,
    )

    all_ccs = notification_emails

    context = {
        "combine_species_qs": combine_species_qs,
        "actions": actions,
        "resulting_species_instance": resulting_species_instance,
        "resulting_species_url": resulting_species_url,
    }

    all_ccs = list(set(all_ccs))

    if resulting_species_instance.submitter:
        submitter_email = EmailUser.objects.get(id=resulting_species_instance.submitter).email
    else:
        submitter_email = None

    to = request.user.email if request else submitter_email

    msg = email.send(
        to,
        cc=all_ccs,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_species_email(msg, resulting_species_instance, sender=sender)

    return msg


# here species_proposal is the original species from rename functionality
def send_species_rename_email_notification(request, species_proposal, new_species):
    email = RenameSpeciesSendNotificationEmail()

    species_url = request.build_absolute_uri(
        reverse(
            "internal-species-detail",
            kwargs={"species_proposal_pk": species_proposal.id},
        )
    )
    species_url = convert_external_url_to_internal_url(species_url)

    new_species_url = request.build_absolute_uri(
        reverse("internal-species-detail", kwargs={"species_proposal_pk": new_species.id})
    )
    new_species_url = convert_external_url_to_internal_url(new_species_url)

    notification_emails = SystemEmailGroup.emails_by_group_and_area(
        group_type=species_proposal.group_type,
    )

    all_ccs = notification_emails

    context = {
        "species_proposal": species_proposal,
        "species_url": species_url,
        "new_species_url": new_species_url,
        "new_species": new_species,
    }

    if species_proposal.submitter:
        submitter_email = EmailUser.objects.get(id=species_proposal.submitter).email
    else:
        submitter_email = None

    to = request.user.email if request else submitter_email

    msg = email.send(
        to,
        cc=all_ccs,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_species_email(msg, species_proposal, sender=sender)

    return msg


def send_community_create_email_notification(request, community_proposal):
    email = CreateCommunitySendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-community-detail",
            kwargs={"community_proposal_pk": community_proposal.id},
        )
    )
    # add the extra query params as need to load the species detail page
    url = url + f"?group_type_name={community_proposal.group_type.name}&action=view"
    url = convert_external_url_to_internal_url(url)

    context = {"species_community_proposal": community_proposal, "url": url}

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=community_proposal.group_type,
    )

    msg = email.send(recipients, context=context)
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_community_email(msg, community_proposal, sender=sender)

    return msg


# created email send to the user who is internal not external
def send_user_community_create_email_notification(request, community_proposal):
    email = UserCreateCommunitySendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-community-detail",
            kwargs={"community_proposal_pk": community_proposal.id},
        )
    )
    # add the extra query params as need to load the species detail page
    url = url + f"?group_type_name={community_proposal.group_type.name}&action=view"

    url = convert_external_url_to_internal_url(url)

    if community_proposal.submitter:
        submitter_user = EmailUser.objects.get(id=community_proposal.submitter)
        submitter_name = submitter_user.get_full_name()
        submitter_email = submitter_user.email
    else:
        submitter_name = "Unknown"
        submitter_email = request.user.email if request else None

    context = {
        "species_community_proposal": community_proposal,
        "submitter": submitter_name,
        "url": url,
    }
    all_ccs = []

    msg = email.send(
        submitter_email,
        cc=all_ccs,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_community_email(msg, community_proposal, sender=sender)

    return msg


def send_community_rename_email_notification(
    request,
    original_community,
    resulting_community,
    original_made_historical,
):
    email = RenameCommunitySendNotificationEmail()

    original_community_url = request.build_absolute_uri(
        reverse(
            "internal-community-detail",
            kwargs={"community_proposal_pk": original_community.id},
        )
    )
    original_community_url = convert_external_url_to_internal_url(original_community_url)

    resulting_community_url = request.build_absolute_uri(
        reverse(
            "internal-community-detail",
            kwargs={"community_proposal_pk": resulting_community.id},
        )
    )
    resulting_community_url = convert_external_url_to_internal_url(resulting_community_url)

    notification_emails = SystemEmailGroup.emails_by_group_and_area(
        group_type=original_community.group_type,
    )

    all_ccs = notification_emails

    context = {
        "original_community": original_community,
        "original_community_url": original_community_url,
        "resulting_community": resulting_community,
        "resulting_community_url": resulting_community_url,
        "original_made_historical": original_made_historical,
    }

    if original_community.submitter:
        submitter_email = EmailUser.objects.get(id=original_community.submitter).email
    else:
        submitter_email = None

    to = request.user.email if request else submitter_email

    msg = email.send(
        to,
        cc=all_ccs,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_community_email(msg, original_community, sender=sender)

    return msg


def send_nomos_script_failed(errors):
    """Internal failed notification email for NOMOS script"""
    email = NomosScriptFailedEmail()

    context = {
        "errors": errors,
    }
    all_ccs = []
    try:
        msg = email.send(settings.NOTIFICATION_EMAIL, cc=all_ccs, context=context)
        return msg
    except Exception as e:
        err_msg = "NOMOS Script Exception Email :"
        logger.error(f"{err_msg}\n{str(e)}")


def _log_species_email(email_message, species_proposal, sender=None, file_bytes=None, filename=None):
    from boranga.components.species_and_communities.models import SpeciesLogEntry

    if isinstance(
        email_message,
        EmailMultiAlternatives | EmailMessage,
    ):
        # Note: this will log the plain text body, should we log the html instead
        text = email_message.body
        subject = email_message.subject
        fromm = smart_str(sender) if sender else smart_str(email_message.from_email)
        # the to email is normally a list
        if isinstance(email_message.to, list):
            to = ",".join(email_message.to)
        else:
            to = smart_str(email_message.to)
        # we log the cc and bcc in the same cc field of the log entry as a ',' comma separated string
        all_ccs = []
        if email_message.cc:
            all_ccs += list(email_message.cc)
        if email_message.bcc:
            all_ccs += list(email_message.bcc)
        all_ccs = ",".join(all_ccs)

    else:
        text = smart_str(email_message)
        subject = ""
        if species_proposal.submitter:
            to = EmailUser.objects.get(id=species_proposal.submitter).email
        else:
            to = None
        fromm = smart_str(sender) if sender else SYSTEM_NAME
        all_ccs = ""

    customer = species_proposal.submitter

    staff = sender.id if sender and hasattr(sender, "id") else None

    kwargs = {
        "subject": subject,
        "text": text,
        "species": species_proposal,
        "customer": customer,
        "staff": staff,
        "to": to,
        "fromm": fromm,
        "cc": all_ccs,
    }

    email_entry = SpeciesLogEntry.objects.create(**kwargs)

    if file_bytes and filename:
        # attach the file to the comms_log also
        path_to_file = f"{settings.MEDIA_APP_DIR}/species/{species_proposal.id}/communications/{filename}"
        private_storage.save(path_to_file, ContentFile(file_bytes))
        email_entry.documents.get_or_create(_file=path_to_file, name=filename)

    return email_entry


def _log_community_email(email_message, community_proposal, sender=None, file_bytes=None, filename=None):
    from boranga.components.species_and_communities.models import CommunityLogEntry

    if isinstance(
        email_message,
        EmailMultiAlternatives | EmailMessage,
    ):
        # Note: this will log the plain text body, should we log the html instead
        text = email_message.body
        subject = email_message.subject
        fromm = smart_str(sender) if sender else smart_str(email_message.from_email)
        # the to email is normally a list
        if isinstance(email_message.to, list):
            to = ",".join(email_message.to)
        else:
            to = smart_str(email_message.to)
        # we log the cc and bcc in the same cc field of the log entry as a ',' comma separated string
        all_ccs = []
        if email_message.cc:
            all_ccs += list(email_message.cc)
        if email_message.bcc:
            all_ccs += list(email_message.bcc)
        all_ccs = ",".join(all_ccs)

    else:
        text = smart_str(email_message)
        subject = ""
        if community_proposal.submitter:
            to = EmailUser.objects.get(id=community_proposal.submitter).email
        else:
            to = None
        fromm = smart_str(sender) if sender else SYSTEM_NAME
        all_ccs = ""

    customer = community_proposal.submitter

    staff = sender.id if sender and hasattr(sender, "id") else None

    kwargs = {
        "subject": subject,
        "text": text,
        "community": community_proposal,
        "customer": customer,
        "staff": staff,
        "to": to,
        "fromm": fromm,
        "cc": all_ccs,
    }

    email_entry = CommunityLogEntry.objects.create(**kwargs)

    if file_bytes and filename:
        # attach the file to the comms_log also
        path_to_file = f"{settings.MEDIA_APP_DIR}/community/{community_proposal.id}/communications/{filename}"
        private_storage.save(path_to_file, ContentFile(file_bytes))
        email_entry.documents.get_or_create(_file=path_to_file, name=filename)

    return email_entry
