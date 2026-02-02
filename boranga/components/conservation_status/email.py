import logging

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from django.core.mail import EmailMessage, EmailMultiAlternatives
from django.urls import reverse
from django.utils import timezone
from django.utils.encoding import smart_str
from ledger_api_client.ledger_models import EmailUserRO as EmailUser

from boranga.components.emails.emails import TemplateEmailBase
from boranga.components.species_and_communities.models import SystemEmailGroup
from boranga.components.users.email import _log_user_email
from boranga.helpers import (
    convert_external_url_to_internal_url,
    convert_internal_url_to_external_url,
    is_internal_by_user_id,
)

private_storage = FileSystemStorage(location=settings.BASE_DIR + "/private-media/", base_url="/private-media/")

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


class SubmitSendNotificationEmail(TemplateEmailBase):
    subject = "A new conservation status proposal has been submitted."
    html_template = "boranga/emails/cs_proposals/send_submit_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_submit_notification.txt"


class ExternalSubmitSendNotificationEmail(TemplateEmailBase):
    subject = f"{settings.DEP_NAME} - confirmation - conservation status proposal submitted."
    html_template = "boranga/emails/cs_proposals/send_external_submit_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_external_submit_notification.txt"


class ConservationStatusReferralSendNotificationEmail(TemplateEmailBase):
    subject = "A referral for a conservation status proposal has been sent to you."
    html_template = "boranga/emails/cs_proposals/send_referral_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_referral_notification.txt"


class ConservationStatusReferralRecallNotificationEmail(TemplateEmailBase):
    subject = "A referral for a conservation status proposal has been recalled."
    html_template = "boranga/emails/cs_proposals/send_referral_recall_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_referral_recall_notification.txt"


class ConservationStatusReferralCompleteNotificationEmail(TemplateEmailBase):
    subject = "A referral for a conservation status has been completed."
    html_template = "boranga/emails/cs_proposals/send_referral_complete_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_referral_complete_notification.txt"


class ConservationStatusAmendmentRequestSendNotificationEmail(TemplateEmailBase):
    subject = "An amendment to your conservation status proposal is required."
    html_template = "boranga/emails/cs_proposals/send_amendment_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_amendment_notification.txt"


class ApproverDeclineSendNotificationEmail(TemplateEmailBase):
    subject = "A conservation status proposal has been recommended for decline."
    html_template = "boranga/emails/cs_proposals/send_approver_decline_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_approver_decline_notification.txt"


class ApproverProposeDelistNotificationEmail(TemplateEmailBase):
    subject = "A conservation status proposal has been proposed for delisting."
    html_template = "boranga/emails/cs_proposals/send_approver_propose_delist_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_approver_propose_delist_notification.txt"


class ApproverApproveSendNotificationEmail(TemplateEmailBase):
    subject = "A conservation status proposal has been recommended for approval."
    html_template = "boranga/emails/cs_proposals/send_approver_approve_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_approver_approve_notification.txt"


class ApproverProposedForAgendaSendNotificationEmail(TemplateEmailBase):
    subject = "A conservation status proposal has been proposed for agenda."
    html_template = "boranga/emails/cs_proposals/send_approver_proposed_for_agenda_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_approver_proposed_for_agenda_notification.txt"


class AssessorReadyForAgendaSendNotificationEmail(TemplateEmailBase):
    subject = "A conservation status proposal is ready for agenda."
    html_template = "boranga/emails/cs_proposals/send_assessor_ready_for_agenda_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_assessor_ready_for_agenda_notification.txt"


class ApproverSendBackNotificationEmail(TemplateEmailBase):
    subject = "A conservation status proposal has been sent back by approver."
    html_template = "boranga/emails/cs_proposals/send_approver_sendback_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_approver_sendback_notification.txt"


class ConservationStatusDeferNotificationEmail(TemplateEmailBase):
    subject = "A conservation status proposal has been deferred."
    html_template = "boranga/emails/cs_proposals/send_defer_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_defer_notification.txt"


class ConservationStatusDeclineSendNotificationEmail(TemplateEmailBase):
    subject = "Your conservation status proposal has been declined."
    html_template = "boranga/emails/cs_proposals/send_decline_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_decline_notification.txt"


class ConservationStatusApprovalSendNotificationEmail(TemplateEmailBase):
    subject = "Your conservation status proposal has been approved."
    html_template = "boranga/emails/cs_proposals/send_approval_notification.html"
    txt_template = "boranga/emails/cs_proposals/send_approval_notification.txt"


def send_submit_email_notification(request, cs_proposal):
    """Recipient: Always internal users"""
    email = SubmitSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-conservation-status-detail",
            kwargs={"cs_proposal_pk": cs_proposal.id},
        )
    )
    url = convert_external_url_to_internal_url(url)

    group_type = cs_proposal.species.group_type if cs_proposal.species else cs_proposal.community.group_type

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    context = {"cs_proposal": cs_proposal, "url": url}

    msg = email.send(recipients, context=context)

    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_conservation_status_email(msg, cs_proposal, sender=sender)

    return msg


def send_submitter_submit_email_notification(request, cs_proposal):
    """Recipient: Maybe internal or external user"""
    email = ExternalSubmitSendNotificationEmail()

    to_user = EmailUser.objects.get(id=cs_proposal.submitter)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-conservation-status-detail",
            kwargs={"cs_proposal_pk": cs_proposal.id},
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    context = {
        "cs_proposal": cs_proposal,
        "submitter": to_user.get_full_name(),
        "url": url,
    }

    msg = email.send(to_user.email, context=context)
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_conservation_status_email(msg, cs_proposal, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_external_referee_invite_email(conservation_status, request, external_referee_invite, reminder=False):
    """Recipient: Always an external user"""

    subject = (
        f"Referral Request for DBCA's Boranga System "
        f"Conservation Status Proposal: {conservation_status.conservation_status_number}"
    )
    if reminder:
        subject = f"Reminder: {subject}"
    email = TemplateEmailBase(
        subject=subject,
        html_template="boranga/emails/cs_proposals/send_external_referee_invite.html",
        txt_template="boranga/emails/cs_proposals/send_external_referee_invite.txt",
    )

    url = request.build_absolute_uri(reverse("home"))

    url = convert_internal_url_to_external_url(url)

    context = {
        "external_referee_invite": external_referee_invite,
        "conservation_status": conservation_status,
        "url": url,
        "reminder": reminder,
    }

    msg = email.send(
        external_referee_invite.email,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    external_referee_invite.datetime_sent = timezone.now()
    external_referee_invite.save()

    return msg


def send_conservation_status_referral_email_notification(referral, request, reminder=False):
    """Recipient: May be internal or external user"""
    email = ConservationStatusReferralSendNotificationEmail()

    to_user = EmailUser.objects.get(id=referral.referral)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-conservation-status-referral-detail",
            kwargs={
                "cs_proposal_pk": referral.conservation_status.id,
                "referral_pk": referral.id,
            },
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    context = {
        "cs_proposal": referral.conservation_status,
        "url": url,
        "reminder": reminder,
        "comments": referral.text,
    }

    msg = email.send(to_user.email, context=context)

    sender = get_sender_user()

    _log_conservation_status_referral_email(msg, referral, to_user.email, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_conservation_status_referral_recall_email_notification(referral, request):
    """Recipient: May be internal or external user"""
    email = ConservationStatusReferralRecallNotificationEmail()

    to_user = EmailUser.objects.get(id=referral.referral)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-conservation-status-referral-detail",
            kwargs={
                "cs_proposal_pk": referral.conservation_status.id,
                "referral_pk": referral.id,
            },
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    context = {
        "cs_proposal": referral.conservation_status,
        "url": url,
    }

    msg = email.send(to_user.email, context=context)

    sender = get_sender_user()

    _log_conservation_status_referral_email(msg, referral, to_user.email, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_conservation_status_referral_complete_email_notification(referral, request):
    """Recipient: Always an internal user"""

    email = ConservationStatusReferralCompleteNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-conservation-status-detail",
            kwargs={"cs_proposal_pk": referral.conservation_status.id},
        )
    )

    to_user = EmailUser.objects.get(id=referral.sent_by)

    context = {
        "cs_proposal": referral.conservation_status,
        "url": url,
        "referral_comments": referral.referral_comment,
    }

    msg = email.send(to_user.email, context=context)

    sender = get_sender_user()

    _log_conservation_status_referral_email(msg, referral, to_user.email, sender=sender)

    return msg


def send_conservation_status_amendment_email_notification(amendment_request, request, conservation_status):
    """Recipient: May be internal or external user"""

    email = ConservationStatusAmendmentRequestSendNotificationEmail()
    reason = amendment_request.reason.reason

    to_user = EmailUser.objects.get(id=conservation_status.submitter)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-conservation-status-detail",
            kwargs={"cs_proposal_pk": conservation_status.id},
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    attachments = []
    if amendment_request.cs_amendment_request_documents:
        for doc in amendment_request.cs_amendment_request_documents.all():
            # file_name = doc._file.name
            file_name = doc.name
            attachment = (file_name, doc._file.file.read())
            attachments.append(attachment)

    context = {
        "cs_proposal": conservation_status,
        "reason": reason,
        "amendment_request_text": amendment_request.text,
        "url": url,
    }

    msg = email.send(
        to_user.email,
        context=context,
        attachments=attachments,
    )

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


# send email when Conservation Status Proposal is 'proposed to decline' by assessor.
def send_approver_decline_email_notification(reason, request, conservation_status):
    """Recipient: Always internal users"""

    email = ApproverDeclineSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-conservation-status-detail",
            kwargs={"cs_proposal_pk": conservation_status.id},
        )
    )
    context = {"cs_proposal": conservation_status, "reason": reason, "url": url}

    group_type = (
        conservation_status.species.group_type
        if conservation_status.species
        else conservation_status.community.group_type
    )

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    return msg


def send_approver_propose_delist_email_notification(request, conservation_status, reason):
    """Recipient: Always internal users"""

    email = ApproverProposeDelistNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-conservation-status-detail",
            kwargs={"cs_proposal_pk": conservation_status.id},
        )
    )
    context = {"cs_proposal": conservation_status, "reason": reason, "url": url}

    group_type = (
        conservation_status.species.group_type
        if conservation_status.species
        else conservation_status.community.group_type
    )

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    return msg


def send_approver_approve_email_notification(request, conservation_status):
    """Recipient: Always internal users"""

    email = ApproverApproveSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-conservation-status-detail",
            kwargs={"cs_proposal_pk": conservation_status.id},
        )
    )
    context = {
        "effective_from_date": conservation_status.conservationstatusissuanceapprovaldetails.effective_from_date,
        "effective_to_date": conservation_status.conservationstatusissuanceapprovaldetails.effective_to_date,
        "details": conservation_status.conservationstatusissuanceapprovaldetails.details,
        "cs_proposal": conservation_status,
        "url": url,
    }

    group_type = (
        conservation_status.species.group_type
        if conservation_status.species
        else conservation_status.community.group_type
    )

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    return msg


def send_approver_proposed_for_agenda_email_notification(request, conservation_status, assessor_comment):
    """Recipient: Always internal users"""

    email = ApproverProposedForAgendaSendNotificationEmail()
    url = request.build_absolute_uri(reverse("internal-meeting-dashboard", kwargs={}))
    context = {
        "cs_proposal": conservation_status,
        "url": url,
        "assessor_comment": assessor_comment,
    }

    group_type = (
        conservation_status.species.group_type
        if conservation_status.species
        else conservation_status.community.group_type
    )

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    return msg


def send_assessor_ready_for_agenda_email_notification(request, conservation_status, assessor_comment):
    """Recipient: Always internal users"""

    email = AssessorReadyForAgendaSendNotificationEmail()
    url = request.build_absolute_uri(reverse("internal-meeting-dashboard", kwargs={}))
    context = {
        "cs_proposal": conservation_status,
        "url": url,
        "assessor_comment": assessor_comment,
    }

    group_type = (
        conservation_status.species.group_type
        if conservation_status.species
        else conservation_status.community.group_type
    )

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    return msg


def send_proposal_approver_sendback_email_notification(request, conservation_status):
    """Recipient: Always internal users"""

    email = ApproverSendBackNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-conservation-status-detail",
            kwargs={"cs_proposal_pk": conservation_status.id},
        )
    )

    if "test-emails" in request.path_info:
        approver_comment = "This is my test comment"
    else:
        approver_comment = conservation_status.approver_comment

    context = {
        "cs_proposal": conservation_status,
        "url": url,
        "approver_comment": approver_comment,
    }

    group_type = (
        conservation_status.species.group_type
        if conservation_status.species
        else conservation_status.community.group_type
    )

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    return msg


def send_approver_defer_email_notification(request, conservation_status, reason):
    """Recipient: Always internal users"""

    email = ConservationStatusDeferNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-conservation-status-detail",
            kwargs={"cs_proposal_pk": conservation_status.id},
        )
    )

    context = {
        "cs_proposal": conservation_status,
        "url": url,
        "reason": reason,
    }

    group_type = (
        conservation_status.species.group_type
        if conservation_status.species
        else conservation_status.community.group_type
    )

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=group_type,
        area=SystemEmailGroup.AREA_CONSERVATION_STATUS,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    return msg


def send_conservation_status_decline_email_notification(conservation_status, conservation_status_decline):
    """Recipient: May be internal or external user Note: Currently does not include a url
    If a url is added in future it must be able to handle both internal and external users
    """

    email = ConservationStatusDeclineSendNotificationEmail()

    context = {
        "cs_proposal": conservation_status,
    }
    cc_list = conservation_status_decline.cc_email
    all_ccs = []
    if cc_list:
        all_ccs = cc_list.split(",")

    to_user = EmailUser.objects.get(id=conservation_status.submitter)

    msg = email.send(to_user.email, bcc=all_ccs, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_conservation_status_approval_email_notification(conservation_status):
    """Recipient: May be internal or external user Note: Currently does not include a url
    If a url is added in future it must be able to handle both internal and external users
    """
    email = ConservationStatusApprovalSendNotificationEmail()

    cc_list = conservation_status.conservationstatusissuanceapprovaldetails.cc_email
    all_ccs = []
    if cc_list:
        all_ccs = cc_list.split(",")

    context = {
        "cs_proposal": conservation_status,
    }

    to_user = EmailUser.objects.get(id=conservation_status.submitter)

    msg = email.send(to_user.email, bcc=all_ccs, context=context)

    sender = get_sender_user()

    _log_conservation_status_email(msg, conservation_status, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def _log_conservation_status_email(email_message, cs_proposal, sender=None, file_bytes=None, filename=None):
    from boranga.components.conservation_status.models import ConservationStatusLogEntry

    if isinstance(
        email_message,
        EmailMultiAlternatives | EmailMessage,
    ):
        # This will log the plain text body
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
        to = EmailUser.objects.get(id=cs_proposal.submitter).email
        fromm = smart_str(sender) if sender else SYSTEM_NAME
        all_ccs = ""

    customer = cs_proposal.submitter

    staff = sender.id

    kwargs = {
        "subject": subject,
        "text": text,
        "conservation_status": cs_proposal,
        "customer": customer,
        "staff": staff,
        "to": to,
        "fromm": fromm,
        "cc": all_ccs,
    }

    email_entry = ConservationStatusLogEntry.objects.create(**kwargs)

    if file_bytes and filename:
        # attach the file to the comms_log also
        path_to_file = f"{settings.MEDIA_APP_DIR}/conservation_status/{cs_proposal.id}/communications/{filename}"
        private_storage.save(path_to_file, ContentFile(file_bytes))
        email_entry.documents.get_or_create(_file=path_to_file, name=filename)

    return email_entry


def _log_conservation_status_referral_email(email_message, referral, to_email, sender=None):
    from boranga.components.conservation_status.models import ConservationStatusLogEntry

    if isinstance(
        email_message,
        EmailMultiAlternatives | EmailMessage,
    ):
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
        to = to_email
        fromm = smart_str(sender) if sender else SYSTEM_NAME
        all_ccs = ""

    customer = referral.referral

    staff = sender.id

    kwargs = {
        "subject": subject,
        "text": text,
        "conservation_status": referral.conservation_status,
        "customer": customer,
        "staff": staff,
        "to": to,
        "fromm": fromm,
        "cc": all_ccs,
    }

    email_entry = ConservationStatusLogEntry.objects.create(**kwargs)

    return email_entry
