import logging

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from boranga.components.conservation_status.email import (
    send_submit_email_notification,
    send_submitter_submit_email_notification,
)
from boranga.components.conservation_status.models import (
    ConservationStatus,
    ConservationStatusAmendmentRequest,
    ConservationStatusUserAction,
)

logger = logging.getLogger(__name__)


@transaction.atomic
def cs_proposal_submit(cs_proposal, request):
    if not cs_proposal.can_user_edit:
        raise ValidationError("You can't edit this proposal at this moment")

    cs_proposal.submitter = request.user.id
    cs_proposal.lodgement_date = timezone.now()

    # Set the status of any pending amendment requests to 'amended'
    cs_proposal.amendment_requests.filter(
        status=ConservationStatusAmendmentRequest.STATUS_CHOICE_REQUESTED
    ).update(status=ConservationStatusAmendmentRequest.STATUS_CHOICE_AMENDED)

    # Create a log entry for the proposal
    cs_proposal.log_user_action(
        ConservationStatusUserAction.ACTION_LODGE_PROPOSAL.format(cs_proposal.id),
        request,
    )

    # Create a log entry for the user
    request.user.log_user_action(
        ConservationStatusUserAction.ACTION_LODGE_PROPOSAL.format(cs_proposal.id),
        request,
    )

    ret1 = send_submit_email_notification(request, cs_proposal)
    ret2 = send_submitter_submit_email_notification(request, cs_proposal)

    if ret1 and ret2:
        cs_proposal.processing_status = (
            ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR
        )
        cs_proposal.customer_status = ConservationStatus.PROCESSING_STATUS_WITH_ASSESSOR
        cs_proposal.save()
    else:
        raise ValidationError(
            "An error occurred while submitting proposal (Submit email notifications failed)"
        )

    return cs_proposal
