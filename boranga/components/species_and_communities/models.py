from django.db import models
from django.db.models.deletion import CASCADE

from boranga.components.species_and_communities.enumerations import GroupTypes


class GroupType(models.Model):
    """
    The three types of group managed by Boranga: fauna, flora and communities. These are the basis
    for all other models in Species and Communities.

    Has a:
    - N/A
    Used by:
    - Species
    Is:
    - Enumeration (GroupTypes)
    """
    id = models.IntegerField(default=-1)
    name = models.CharField(max_length=4,
                            choices=GroupTypes.choices,
                            default=GroupTypes.FAUNA,)


class ConservationList(models.Model):
    """
    Description TBC

    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    id = models.IntegerField(default=-1)
    name = models.CharField(max_length=128,
                            default="None")


class ConservationCategory(models.Model):
    """
    Description TBC

    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    id = models.IntegerField(default=-1)
    name = models.CharField(max_length=128,
                            default="None")


class ConservationCriteria(models.Model):
    """
    Description TBC

    Has a:
    - N/A
    Used by:
    - ConservationStatus
    Is:
    - TBD
    """
    id = models.IntegerField(default=-1)
    name = models.CharField(max_length=128,
                            default="None")


class ConservationStatus(models.Model):
    """
    Description TBC

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    conservation_list = models.OneToOneField(ConservationList,
                                             on_delete=models.CASCADE,
                                             primary_key=True,)
    conservation_category = models.ForeignKey(ConservationCategory, on_delete=CASCADE)
    conservation_criteria = models.ForeignKey(ConservationCriteria, on_delete=CASCADE)


class Taxonomy(models.Model):
    """
    Description TBC

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    id = models.IntegerField(default=-1)
    taxon_id = models.IntegerField(default=-1)
    species_id = models.IntegerField(default=-1)
    previous_name = models.CharField(max_length=512,
                                     default="None")
    family = models.CharField(max_length=512,
                              default="None")
    genus = models.CharField(max_length=512,
                             default="None")
    phylogenetic_group = models.CharField(max_length=512,
                                          default="None")
    name_authority = models.CharField(max_length=512,
                                      default="None")
    community_id = models.IntegerField(default=-1)
    community_number = models.IntegerField(default=-1)
    community_description = models.CharField(max_length=512,
                                             default="None")


class Species(models.Model):
    """
    Forms the basis for a Species and Communities record.

    Has a:
    - ConservationStatus
    - Community
    - GroupType
    - SpeciesDocument
    - ConservationThreat
    - ConservationPlan
    - Taxonomy
    - Distribution
    - ConservationAttributes
    Used by:
    - Communities
    Is:
    - Table
    """
    group_type = models.ForeignKey(GroupType,
                                   on_delete=models.CASCADE,
                                   primary_key=True,)
    scientific_name = models.CharField(max_length=128,
                                       default="None")
    common_name = models.CharField(max_length=128,
                                   default="None")
    name_currency = models.CharField(max_length=512,
                                     default="None")
    conservation_status = models.OneToOneField(ConservationStatus,
                                               on_delete=models.CASCADE,
                                               primary_key=True,)
    # community many to many
    region = models.IntegerField(default=-1)            #TODO: reuse DBCA
    district = models.IntegerField(default=-1)          #TODO: reuse DBCA
    image = models.CharField(max_length=512,
                             default="None")
    processing_status = models.CharField(max_length=512,
                                         default="None")
    # species_document foreign key
    # conservation_threats foreign key
    # conservation_plans many to many
    taxonomy = models.OneToOneField(Taxonomy, on_delete=models.CASCADE,)
    # distribution one to one
    # conservation_attributes one to one


class Community(models.Model):
    """
    A collection of 2 or more Species within a specific location.

    Has a:
    - ConservationStatus
    - Species
    Used by:
    - N/A
    Is:
    - Table
    """
    id = models.IntegerField(default=-1)
    community_name = models.CharField(max_length=128,
                                      default="None")
    community_id = models.IntegerField(default=-1)
    community_status = models.CharField(max_length=128,
                                        default="None")
    region_id = models.IntegerField(default=-1)
    district_id = models.IntegerField(default=-1)

    species = models.ManyToManyField(Species, blank=False)
    conservation_status = models.ForeignKey(to=ConservationStatus, on_delete=CASCADE)

    community = models.ManyToManyField(Species, blank=False)


class SpeciesDocument(models.Model):
    """
    Meta-data associated with a document relevant to a Species.

    Has a:
    - DocumentCategory
    Used by:
    - Species
    - Communities:
    Is:
    - Table
    """
    species_id = models.IntegerField(default=-1)
    category_id = models.IntegerField(default=-1)
    status = models.CharField(max_length=512,
                              default="None")
    document = models.CharField(max_length=512,
                                default="None")
    document_description = models.CharField(max_length=1024,
                                            default="None")
    date_time = models.DateField()

    species = models.ManyToManyField(Species, blank=False)


class DocumentCategory(models.Model):
    """
    Description TBC

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    id = models.IntegerField(default=-1)
    name = models.CharField(max_length=128,
                            default="None")

    species_document = models.ForeignKey(to=SpeciesDocument, on_delete=CASCADE)


class ConservationThreat(models.Model):
    """
    Description TBC

    Has a:
    - N/A
    Used by:
    - Species
    Is:
    - Table
    """
    id = models.IntegerField(default=-1)
    species_id = models.IntegerField(default=-1)
    threat_category_id = models.IntegerField(default=-1)
    threat_description = models.CharField(max_length=512,
                                          default="None")
    comment = models.CharField(max_length=512,
                               default="None")
    document = models.CharField(max_length=1024,
                                default="None")
    source = models.CharField(max_length=1024,
                              default="None")

    species = models.ForeignKey(Species, on_delete=CASCADE)


class ConservationPlan(models.Model):
    """
    Description TBC

    Has a:
    - N/A
    Used by:
    - Species
    Is:
    - Table
    """
    id = models.IntegerField(default=-1)
    species_id = models.IntegerField(default=-1)
    threat_category_id = models.IntegerField(default=-1)
    region_id = models.IntegerField(default=-1)
    district_id = models.IntegerField(default=-1)
    type = models.CharField(max_length=512,
                            default="None")
    comment = models.CharField(max_length=512,
                               default="None")
    source = models.CharField(max_length=1024,
                              default="None")

    species = models.ManyToManyField(Species, blank=False)


class ConservationAttributes(models.Model):
    """
    Description TBC

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    id = models.IntegerField(default=-1)
    species_id = models.IntegerField(default=-1)
    general_management_advice = models.CharField(max_length=512,
                                                 default="None")
    ecological_attributes = models.CharField(max_length=512,
                                             default="None")
    biological_attributes = models.CharField(max_length=512,
                                             default="None")
    specific_survey_advice = models.CharField(max_length=512,
                                              default="None")

    species = models.OneToOneField(Species,
                                   on_delete=models.CASCADE,
                                   primary_key=True,)


class Distribution(models.Model):
    """
    Description TBC

    Has a:
    - ConservationList
    - ConservationCategory
    - ConservationCriteria
    Used by:
    - Species
    - Communities
    Is:
    - Table
    """
    department_file_numbers = models.IntegerField(default=-1)
    community_original_area = models.IntegerField(default=-1)
    community_original_area_accuracy = models.IntegerField(default=-1)
    number_of_occurrences = models.IntegerField(default=-1)
    extent_of_occurrences = models.IntegerField(default=-1)
    area_of_occupancy = models.IntegerField(default=-1)
    number_of_iucn_locations = models.IntegerField(default=-1)
    community_original_area_reference = models.CharField(max_length=512,
                                                         default="None")
    species = models.OneToOneField(Species,
                                   on_delete=models.CASCADE,
                                   primary_key=True,)

