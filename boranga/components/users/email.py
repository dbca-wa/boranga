import logging

from django.conf import settings
from django.core.mail import EmailMessage, EmailMultiAlternatives
from django.utils.encoding import smart_str

SYSTEM_NAME = settings.SYSTEM_NAME_SHORT + " Automated Message"


logger = logging.getLogger(__name__)


def _log_user_email(email_message, emailuser, customer, sender=None):
    from boranga.components.users.models import EmailUserLogEntry

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
        to = customer
        fromm = smart_str(sender) if sender else SYSTEM_NAME
        all_ccs = ""

    staff_id = None
    if sender and hasattr(sender, "id"):
        staff_id = sender.id

    kwargs = {
        "subject": subject,
        "text": text,
        "email_user": emailuser.id,
        "customer": customer.id,
        "staff": staff_id,
        "to": to,
        "fromm": fromm,
        "cc": all_ccs,
    }

    email_entry = EmailUserLogEntry.objects.create(**kwargs)

    return email_entry
