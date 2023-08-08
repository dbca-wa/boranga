import logging
import datetime
from django.utils import timezone
from django.db import models
from django.core.exceptions import ValidationError
from django.contrib.postgres.fields.jsonb import JSONField
from boranga import exceptions
from boranga.components.main.models import (
    CommunicationsLogEntry, 
    UserAction,
    Document,
    RevisionedMixin,
)


from boranga.ledger_api_utils import retrieve_email_user
from ledger_api_client.ledger_models import EmailUserRO as EmailUser
from ledger_api_client.managed_models import SystemGroup
import json
from django.db import models,transaction
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from boranga.settings import (
    GROUP_NAME_ASSESSOR,
    GROUP_NAME_APPROVER,
    GROUP_NAME_EDITOR,
)
from boranga.components.main.utils import get_department_user
from boranga.components.main.related_item import RelatedItem
from boranga.components.species_and_communities.models import (
    DocumentCategory,
    DocumentSubCategory,
)
from boranga.components.conservation_status.models import ConservationStatus
from boranga.ordered_model import OrderedModel

logger = logging.getLogger(__name__)

def update_meeting_comms_log_filename(instance, filename):
    return '{}/meeting/{}/communications/{}'.format(settings.MEDIA_APP_DIR, instance.log_entry.meeting.id,filename)

private_storage = FileSystemStorage(location=settings.BASE_DIR+"/private-media/", base_url='/private-media/')

def update_meeting_doc_filename(instance, filename):
    return '{}/meeting/{}/meeting_minutes_document/{}'.format(settings.MEDIA_APP_DIR, instance.meeting.id,filename)

class MeetingRoom(models.Model):
    """

    The Room(Location) for a individual meeting scheduled
    The Admin data

    """
    room_name = models.CharField(max_length=128, blank=True, null=True)

    class Meta:
        app_label = 'boranga'
        verbose_name = "Meeting Room"
        verbose_name_plural = "Meeting Rooms"
        ordering = ['room_name']

    def __str__(self):
        return str(self.room_name)


class Committee(models.Model):
    """

    The Commitee used for the meeting attendees 
    The Admin data

    """
    name = models.CharField(max_length=328, blank=True, null=True)
    # members = models.ManyToManyField(CommitteeMembers, null=True, blank=True, related_name='committee_members')

    class Meta:
        app_label = 'boranga'
        ordering = ['name']

    def __str__(self):
        return str(self.name)


class CommitteeMembers(models.Model):
    """

    The Committee members info
    The Admin data

    """
    first_name = models.CharField(max_length=128, blank=True, null=True)
    last_name = models.CharField(max_length=128, blank=True, null=True)
    email = models.CharField(max_length=328, blank=True, null=True)
    committee = models.ForeignKey('Committee', on_delete=models.CASCADE, null=True, blank= True)

    class Meta:
        app_label = 'boranga'
        verbose_name = "Committee"
        verbose_name_plural = "Committee Members"

    def __str__(self):
        return str(self.email)


class Meeting(models.Model):
    """
    A list of conservation status for a species is executed during Meetings or Committee Meetings. 
    It is necessary to capture these changes and the meetings that caused the change. 

    Has a:
    - Contact
    """
    MEETING = 'meeting'
    COMMITTEE_MEETING = 'committee_meeting'
    PROCESSING_STATUS_DRAFT='draft'
    PROCESSING_STATUS_SCHEDULED='scheduled'


    MEETING_TYPE_CHOICES = (
        (MEETING, 'Meeting'),
        (COMMITTEE_MEETING, 'Committee Meeting'),
        
    )
    PROCESSING_STATUS_CHOICES = ((PROCESSING_STATUS_DRAFT, 'Draft'),
                                 (PROCESSING_STATUS_SCHEDULED, 'Scheduled'),
                                )
    meeting_number = models.CharField(max_length=9, blank=True, default='')
    meeting_type = models.CharField('Meeting Type', max_length=30, choices=MEETING_TYPE_CHOICES,
                                     default=MEETING_TYPE_CHOICES[0][0])
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    location = models.ForeignKey(MeetingRoom, on_delete=models.SET_NULL, null=True, blank=True, related_name="meetings")
    title = models.CharField(max_length=128, blank=True, null=True)
    attendees = models.CharField(max_length=1208, blank=True, null=True)
    # if commitee meeting 
    committee = models.ForeignKey(Committee, on_delete=models.SET_NULL, null=True, blank=True, related_name="committee")
    selected_committee_members = models.ManyToManyField(CommitteeMembers, null=True, blank=True)
    # Agenda items are all conservationstatus added to the meeting
    # the below agenda field is not used to agenda items
    # agenda = models.ManyToManyField(ConservationStatus, null=True, blank=True)
    processing_status = models.CharField('Processing Status', max_length=30, choices=PROCESSING_STATUS_CHOICES,
                                         default=PROCESSING_STATUS_CHOICES[0][0])
    lodgement_date = models.DateTimeField(blank=True, null=True)
    submitter = models.IntegerField(null=True) #EmailUserRO
                                
    class Meta:
        app_label = 'boranga'

    def __str__(self):
        return str(self.title)
    
    def save(self, *args, **kwargs):
        # Prefix "M" char to meeting_number.
        super(Meeting, self).save(*args,**kwargs)
        if self.meeting_number == '':
            new_meeting_id = 'M{}'.format(str(self.pk))
            self.meeting_number = new_meeting_id
            self.save()

    @property
    def reference(self):
        return '{}-{}'.format(self.meeting_number, self.meeting_number)
    
    def log_user_action(self, action, request):
        return MeetingUserAction.log_action(self, action, request.user.id)
    
    @property
    def can_user_edit(self):
        """
        :return: True if the application is in one of the editable status.
        """
        # return self.customer_status in self.CUSTOMER_EDITABLE_STATE
        user_editable_state = ['draft',]
        return self.processing_status in user_editable_state
    
    def submit(self,request,viewset):
        with transaction.atomic():
            if self.can_user_edit:
                # Save the data first
                # save_proponent_data(self,request,viewset)
                # Check if the special fields have been completed
                # missing_fields = self.__check_proposal_filled_out()
                # if missing_fields:
                #     error_text = 'The proposal has these missing fields, {}'.format(','.join(missing_fields))
                #     raise exceptions.ProposalMissingFields(detail=error_text)
                self.submitter = request.user.id
                self.lodgement_date = timezone.now()
                # Create a log entry for the proposal
                self.log_user_action(MeetingUserAction.ACTION_CREATE_MEETING.format(self.id),request)
                # Create a log entry for the organisation
                # TODO handle the error "'EmailUserRO' object has no attribute 'log_user_action'" for below
                # applicant_field=getattr(self, self.applicant_field)
                # applicant_field.log_user_action(ProposalUserAction.ACTION_LODGE_APPLICATION.format(self.id),request)

                # ret1 = send_submit_email_notification(request, self)
                # ret2 = send_external_submit_email_notification(request, self)

                # if ret1 and ret2:
                #     self.processing_status = 'with_assessor'
                #     self.customer_status = 'with_assessor'
                #     self.documents.all().update(can_delete=False)
                #     self.save()
                self.processing_status = 'scheduled'
                # self.documents.all().update(can_delete=False)
                self.save()
                # else:
                #     raise ValidationError('An error occurred while submitting proposal (Submit email notifications failed)')
            else:
                raise ValidationError('You can\'t edit this meeting at this moment')


class MeetingLogDocument(Document):
    log_entry = models.ForeignKey('MeetingLogEntry',related_name='documents', on_delete=models.CASCADE)
    _file = models.FileField(upload_to=update_meeting_comms_log_filename, max_length=512, storage=private_storage)

    class Meta:
        app_label = 'boranga'


class MeetingLogEntry(CommunicationsLogEntry):
    meeting = models.ForeignKey(Meeting, related_name='comms_logs', on_delete=models.CASCADE)

    def __str__(self):
        return '{} - {}'.format(self.reference, self.subject)

    class Meta:
        app_label = 'boranga'

    def save(self, **kwargs):
        # save the application reference if the reference not provided
        if not self.reference:
            self.reference = self.meeting.reference
        super(MeetingLogEntry, self).save(**kwargs)


class MeetingUserAction(UserAction):
    
    ACTION_EDIT_SPECIES= "Edit Species {}"
    ACTION_CREATE_MEETING= "Create new meeting {}"
    ACTION_SAVE_MEETING = "Save Meeting {}"

    # Minutes Document
    ACTION_ADD_MINUTE= "Minutes {} added for Meeting {}"
    ACTION_UPDATE_MINUTE= "Minutes {} updated for Meeting {}"
    ACTION_DISCARD_MINUTE= "Minutes {} discarded for Meeting {}"
    ACTION_REINSTATE_MINUTE= "Minutes {} reinstated for Meeting {}"

    ACTION_DISCARD_MEETING = "Discard Meeting {}"

    class Meta:
        app_label = 'boranga'
        ordering = ('-when',)

    @classmethod
    def log_action(cls, meeting, action, user):
        return cls.objects.create(
            meeting=meeting,
            who=user,
            what=str(action)
        )

    meeting = models.ForeignKey(Meeting, related_name='action_logs', on_delete=models.CASCADE)


class Minutes(Document):
    """
    Meta-data associated with a document relevant to a Meeting.

    Has a:
    - Meeting
    - DocumentCategory
    - DocumentSubCategoty
    Used for:
    - Meeting
    Is:
    - Table
    """
    minutes_number = models.CharField(max_length=9, blank=True, default='')
    _file = models.FileField(upload_to=update_meeting_doc_filename, max_length=512, default="None", storage=private_storage)
    input_name = models.CharField(max_length=255,null=True,blank=True)
    can_delete = models.BooleanField(default=True) # after initial submit prevent document from being deleted
    visible = models.BooleanField(default=True) # to prevent deletion on file system, hidden and still be available in history 
    document_category = models.ForeignKey(DocumentCategory, 
                                          null=True,
                                          blank=True,
                                          on_delete=models.SET_NULL)
    document_sub_category = models.ForeignKey(DocumentSubCategory, 
                                          null=True,
                                          blank=True,
                                          on_delete=models.SET_NULL)
    meeting = models.ForeignKey(Meeting, 
                                blank=False, 
                                default=None,
                                on_delete=models.CASCADE,
                                related_name='meeting_minutes')

    class Meta:
        app_label = 'boranga'
        verbose_name = "Meeting Minutes"

    def save(self, *args, **kwargs):
        # Prefix "MN" char to minutes_number.
        super(Minutes, self).save(*args,**kwargs)
        if self.minutes_number == '':
            new_minute_id = 'MN{}'.format(str(self.pk))
            self.minutes_number = new_minute_id
            self.save()
    
    def add_minutes_documents(self, request):
        with transaction.atomic():
            try:
                # save the files
                data = json.loads(request.data.get('data'))
                # if not data.get('update'):
                #     documents_qs = self.filter(input_name='species_doc', visible=True)
                #     documents_qs.delete()
                for idx in range(data['num_files']):
                    _file = request.data.get('file-'+str(idx))
                    self._file=_file
                    self.name=_file.name
                    self.input_name = data['input_name']
                    self.can_delete = True
                    self.save()
                # end save documents
                self.save()
            except:
                raise
        return


class AgendaItem(OrderedModel):
    meeting = models.ForeignKey(Meeting,on_delete=models.CASCADE,related_name='agenda_items')
    conservation_status = models.ForeignKey(ConservationStatus, on_delete=models.CASCADE)
    # order = models.IntegerField(null=True, blank=True)

    class Meta:
        app_label = 'boranga'
        # the verbose name should be meeting as used in the Related items tab of CS
        verbose_name = "Meeting Agenda Item" 
        unique_together = ("meeting", "conservation_status")
        ordering = ["meeting", "order"]
        # constraints = [
        #     models.UniqueConstraint(
        #         fields=["meeting", "order"],
        #         name="unique agenda order per Meeting",
        #     )
        # ]
    
    def __str__(self):
        return str(self.meeting)

    @property
    def as_related_item(self):
        related_item = RelatedItem(
            identifier=self.related_item_identifier,
            model_name=self._meta.verbose_name,
            descriptor=self.related_item_descriptor,
            status=self.related_item_status,
            action_url='<a href=/internal/meetings/{} target="_blank">View</a>'.format(self.meeting.id)
        )
        return related_item

    @property
    def related_item_identifier(self):
        return self.meeting.meeting_number

    @property
    def related_item_descriptor(self):
        return self.meeting.title

    @property
    def related_item_status(self):
        return self.meeting.get_processing_status_display

