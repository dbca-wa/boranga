import logging

from django.conf import settings
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
    subject = "A new Occurrence Report has been submitted."
    html_template = "boranga/emails/ocr_proposals/send_submit_notification.html"
    txt_template = "boranga/emails/ocr_proposals/send_submit_notification.txt"


class ExternalSubmitSendNotificationEmail(TemplateEmailBase):
    subject = "{} - Confirmation - Occurrence Report submitted.".format(
        settings.DEP_NAME
    )
    html_template = (
        "boranga/emails/ocr_proposals/send_external_submit_notification.html"
    )
    txt_template = "boranga/emails/ocr_proposals/send_external_submit_notification.txt"


class OccurrenceReportReferralSendNotificationEmail(TemplateEmailBase):
    subject = "A referral for an occurrence report has been sent to you."
    html_template = "boranga/emails/ocr_proposals/send_referral_notification.html"
    txt_template = "boranga/emails/ocr_proposals/send_referral_notification.txt"


class OccurrenceReportReferralRecallNotificationEmail(TemplateEmailBase):
    subject = "A referral for an occurrence report has been recalled."
    html_template = (
        "boranga/emails/ocr_proposals/send_referral_recall_notification.html"
    )
    txt_template = "boranga/emails/ocr_proposals/send_referral_recall_notification.txt"


class OccurrenceReportReferralCompleteNotificationEmail(TemplateEmailBase):
    subject = "A referral for an occurrence report has been completed."
    html_template = (
        "boranga/emails/ocr_proposals/send_referral_complete_notification.html"
    )
    txt_template = (
        "boranga/emails/ocr_proposals/send_referral_complete_notification.txt"
    )


class OccurrenceReportAmendmentRequestSendNotificationEmail(TemplateEmailBase):
    subject = "An amendment to your occurrence report is required."
    html_template = "boranga/emails/ocr_proposals/send_amendment_notification.html"
    txt_template = "boranga/emails/ocr_proposals/send_amendment_notification.txt"


class ApproverDeclineSendNotificationEmail(TemplateEmailBase):
    subject = "An Assessor has proposed to decline an Occurrence Report"
    html_template = (
        "boranga/emails/ocr_proposals/send_approver_decline_notification.html"
    )
    txt_template = "boranga/emails/ocr_proposals/send_approver_decline_notification.txt"


class DeclineSendNotificationEmail(TemplateEmailBase):
    subject = "Occurrence Report Declined"
    html_template = "boranga/emails/ocr_proposals/send_decline_notification.html"
    txt_template = "boranga/emails/ocr_proposals/send_decline_notification.txt"


class ApproveSendNotificationEmail(TemplateEmailBase):
    subject = "Occurrence Report Approved"
    html_template = "boranga/emails/ocr_proposals/send_approval_notification.html"
    txt_template = "boranga/emails/ocr_proposals/send_approval_notification.txt"


class ApproverApproveSendNotificationEmail(TemplateEmailBase):
    subject = "An Occurrence Report has been recommended for approval."
    html_template = (
        "boranga/emails/ocr_proposals/send_approver_approve_notification.html"
    )
    txt_template = "boranga/emails/ocr_proposals/send_approver_approve_notification.txt"


class ApproverBackToAssessorSendNotificationEmail(TemplateEmailBase):
    subject = "An Occurrence Report has been sent back to the Assessor."
    html_template = (
        "boranga/emails/ocr_proposals/send_approver_back_to_assessor_notification.html"
    )
    txt_template = (
        "boranga/emails/ocr_proposals/send_approver_back_to_assessor_notification.txt"
    )


def send_submit_email_notification(request, occurrence_report):
    """Recipient: Always internal users"""

    email = SubmitSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-occurrence-report-detail",
            kwargs={"occurrence_report_pk": occurrence_report.id},
        )
    )

    url = convert_external_url_to_internal_url(url)

    context = {"occurrence_report": occurrence_report, "url": url}

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=occurrence_report.group_type,
        area=SystemEmailGroup.AREA_OCCURRENCE,
    )

    msg = email.send(recipients, context=context)

    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    return msg


def send_submitter_submit_email_notification(request, occurrence_report):
    """Recipient: Maybe internal or external user"""

    email = ExternalSubmitSendNotificationEmail()

    to_user = EmailUser.objects.get(id=occurrence_report.submitter)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-occurrence-report-detail",
            kwargs={"occurrence_report_pk": occurrence_report.id},
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    context = {
        "occurrence_report": occurrence_report,
        "submitter": EmailUser.objects.get(
            id=occurrence_report.submitter
        ).get_full_name(),
        "url": url,
    }

    msg = email.send(
        EmailUser.objects.get(id=occurrence_report.submitter).email,
        cc=None,
        context=context,
    )

    sender = request.user if request else settings.DEFAULT_FROM_EMAIL

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_external_referee_invite_email(
    occurrence_report, request, external_referee_invite, reminder=False
):
    """Recipient: Always an external user"""

    subject = (
        f"Referral Request for DBCA's Boranga System "
        f"Occurrence Report: {occurrence_report.occurrence_report_number}"
    )
    if reminder:
        subject = f"Reminder: {subject}"
    email = TemplateEmailBase(
        subject=subject,
        html_template="boranga/emails/ocr_proposals/send_external_referee_invite.html",
        txt_template="boranga/emails/ocr_proposals/send_external_referee_invite.txt",
    )

    url = request.build_absolute_uri(reverse("home"))

    url = convert_internal_url_to_external_url(url)

    context = {
        "external_referee_invite": external_referee_invite,
        "occurrence_report": occurrence_report,
        "url": url,
        "reminder": reminder,
    }

    msg = email.send(
        external_referee_invite.email,
        context=context,
    )
    sender = request.user if request else settings.DEFAULT_FROM_EMAIL
    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    external_referee_invite.datetime_sent = timezone.now()
    external_referee_invite.save()

    return msg


def send_occurrence_report_referral_email_notification(
    referral, request, reminder=False
):
    """Recipient: May be internal or external user"""

    email = OccurrenceReportReferralSendNotificationEmail()

    to_user = EmailUser.objects.get(id=referral.referral)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-occurrence-report-referral-detail",
            kwargs={
                "occurrence_report_pk": referral.occurrence_report.id,
                "referral_pk": referral.id,
            },
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    context = {
        "occurrence_report": referral.occurrence_report,
        "url": url,
        "reminder": reminder,
        "comments": referral.text,
    }

    msg = email.send(to_user.email, context=context)

    sender = get_sender_user()

    _log_occurrence_report_referral_email(msg, referral, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_occurrence_report_referral_recall_email_notification(referral, request):
    """Recipient: May be internal or external user"""

    email = OccurrenceReportReferralRecallNotificationEmail()

    to_user = EmailUser.objects.get(id=referral.referral)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-occurrence-report-referral-detail",
            kwargs={
                "occurrence_report_pk": referral.occurrence_report.id,
                "referral_pk": referral.id,
            },
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    context = {
        "occurrence_report": referral.occurrence_report,
        "url": url,
    }

    msg = email.send(to_user.email, context=context)

    sender = get_sender_user()

    _log_occurrence_report_referral_email(msg, referral, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_occurrence_report_referral_complete_email_notification(referral, request):
    """Recipient: Always an internal user"""

    email = OccurrenceReportReferralCompleteNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-occurrence-report-detail",
            kwargs={"occurrence_report_pk": referral.occurrence_report.id},
        )
    )

    email_address = EmailUser.objects.get(id=referral.sent_by).email

    context = {
        "occurrence_report": referral.occurrence_report,
        "url": url,
        "referral_comments": referral.referral_comment,
    }

    msg = email.send(email_address, context=context)

    sender = get_sender_user()

    _log_occurrence_report_referral_email(msg, referral, sender=sender)

    return msg


def send_occurrence_report_amendment_email_notification(
    amendment_request, request, occurrence_report
):
    """Recipient: May be internal or external user"""

    email = OccurrenceReportAmendmentRequestSendNotificationEmail()
    reason = amendment_request.reason.reason

    to_user = EmailUser.objects.get(id=occurrence_report.submitter)

    url_name_prefix = "internal"

    if not is_internal_by_user_id(to_user.id):
        url_name_prefix = "external"

    url = request.build_absolute_uri(
        reverse(
            f"{url_name_prefix}-occurrence-report-detail",
            kwargs={"occurrence_report_pk": occurrence_report.id},
        )
    )

    if not is_internal_by_user_id(to_user.id):
        url = convert_internal_url_to_external_url(url)

    attachments = []
    if amendment_request.amendment_request_documents:
        for doc in amendment_request.amendment_request_documents.all():
            # file_name = doc._file.name
            file_name = doc.name
            attachment = (file_name, doc._file.file.read())
            attachments.append(attachment)

    context = {
        "occurrence_report": occurrence_report,
        "reason": reason,
        "amendment_request_text": amendment_request.text,
        "url": url,
    }

    msg = email.send(
        to_user.email,
        cc=[],
        context=context,
        attachments=attachments,
    )

    sender = get_sender_user()

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


# send email when Occurrence Report is 'proposed to decline' by assessor.
def send_approver_decline_email_notification(reason, request, occurrence_report):
    """Recipient: Always internal users"""

    email = ApproverDeclineSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-occurrence-report-detail",
            kwargs={"occurrence_report_pk": occurrence_report.id},
        )
    )
    context = {"occurrence_report": occurrence_report, "reason": reason, "url": url}

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=occurrence_report.group_type,
        area=SystemEmailGroup.AREA_OCCURRENCE,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    return msg


def send_decline_email_notification(reason, occurrence_report):
    """Recipient: May be internal or external user Note: Currently does not include a url
    If a url is added in future it must be able to handle both internal and external users
    """

    email = DeclineSendNotificationEmail()

    context = {"occurrence_report": occurrence_report, "reason": reason}

    to_user = EmailUser.objects.get(id=occurrence_report.submitter)

    cc_list = occurrence_report.declined_details.cc_email
    all_ccs = []
    if cc_list:
        all_ccs = cc_list.split(",")

    msg = email.send(to_user.email, context=context, cc=all_ccs)

    sender = get_sender_user()

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


def send_approve_email_notification(occurrence_report):
    """Recipient: May be internal or external user Note: Currently does not include a url
    If a url is added in future it must be able to handle both internal and external users
    """

    email = ApproveSendNotificationEmail()

    context = {"occurrence_report": occurrence_report}

    to_user = EmailUser.objects.get(id=occurrence_report.submitter)

    cc_list = occurrence_report.approval_details.cc_email
    all_ccs = []
    if cc_list:
        all_ccs = cc_list.split(",")

    msg = email.send(to_user.email, context=context, cc=all_ccs)

    sender = get_sender_user()

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    _log_user_email(msg, to_user, to_user, sender=sender)

    return msg


# send email when Occurrence Report is 'proposed to approve' by assessor.
def send_approver_approve_email_notification(request, occurrence_report):
    """Recipient: Always internal users"""

    email = ApproverApproveSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-occurrence-report-detail",
            kwargs={"occurrence_report_pk": occurrence_report.id},
        )
    )
    approval_details = occurrence_report.approval_details

    context = {
        "occurrence_report": occurrence_report,
        "approval_details": approval_details,
        "url": url,
    }

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=occurrence_report.group_type,
        area=SystemEmailGroup.AREA_OCCURRENCE,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    return msg


def send_approver_back_to_assessor_email_notification(
    request, occurrence_report, reason
):
    """Recipient: Always internal users"""

    email = ApproverBackToAssessorSendNotificationEmail()
    url = request.build_absolute_uri(
        reverse(
            "internal-occurrence-report-detail",
            kwargs={"occurrence_report_pk": occurrence_report.id},
        )
    )

    context = {
        "occurrence_report": occurrence_report,
        "reason": reason,
        "url": url,
    }

    recipients = SystemEmailGroup.emails_by_group_and_area(
        group_type=occurrence_report.group_type,
        area=SystemEmailGroup.AREA_OCCURRENCE,
    )

    msg = email.send(recipients, context=context)

    sender = get_sender_user()

    _log_occurrence_report_email(msg, occurrence_report, sender=sender)

    return msg


def _log_occurrence_report_email(email_message, occurrence_report, sender=None):
    from boranga.components.occurrence.models import OccurrenceReportLogEntry

    if isinstance(
        email_message,
        (
            EmailMultiAlternatives,
            EmailMessage,
        ),
    ):
        # Note: this will log the plain text body
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
        to = EmailUser.objects.get(id=occurrence_report.submitter).email
        fromm = smart_str(sender) if sender else SYSTEM_NAME
        all_ccs = ""

    customer = occurrence_report.submitter

    staff = sender.id

    kwargs = {
        "subject": subject,
        "text": text,
        "occurrence_report": occurrence_report,
        "customer": customer,
        "staff": staff,
        "to": to,
        "fromm": fromm,
        "cc": all_ccs,
    }

    email_entry = OccurrenceReportLogEntry.objects.create(**kwargs)

    return email_entry


def _log_occurrence_report_referral_email(email_message, referral, sender=None):
    from boranga.components.occurrence.models import OccurrenceReportLogEntry

    if isinstance(
        email_message,
        (
            EmailMultiAlternatives,
            EmailMessage,
        ),
    ):
        # Note: this will log the plain text body
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
        to = EmailUser.objects.get(id=referral.referral).email
        fromm = smart_str(sender) if sender else SYSTEM_NAME
        all_ccs = ""

    customer = referral.referral

    staff = sender.id

    kwargs = {
        "subject": subject,
        "text": text,
        "occurrence_report": referral.occurrence_report,
        "customer": customer,
        "staff": staff,
        "to": to,
        "fromm": fromm,
        "cc": all_ccs,
    }

    email_entry = OccurrenceReportLogEntry.objects.create(**kwargs)

    return email_entry
