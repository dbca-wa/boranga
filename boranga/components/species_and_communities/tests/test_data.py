
from contextlib import suppress
import csv
import datetime
from dis import dis
from random import randrange

import django

# Create your tests here.
from boranga.components.species_and_communities.models import DISTRICT_SWAN_COASTAL, REGION_CHOICES, REGION_SOUTH_WEST, CommitteeMeeting, ConservationChangeCode, Contact, District, DocumentCategory, NameAuthority, Region, Species, GroupType, ConservationStatus, ConservationList, \
    ConservationCategory, ConservationCriteria, SpeciesAttributes, Taxonomy, Community, SpeciesDocument, ConservationThreat, \
    ConservationPlan, Distribution, ConservationAttributes, ThreatCategory

import logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(#file="/home/graeme/workspace/boranga/boranga/components/species_and_communities/tests/debug.log",
                    format='create_test_data: %(message)s',
                    filemode='w')
logger = logging.getLogger()

save_to_database = False

def create_test_data():
    print('----------------------------------------------------')
    print('--------------ADDING TEST DATA----------------------')
    print('----------------------------------------------------')
    create_change_codes()
    

def create_committee_meetings():
    print('--------------Starting {}'.format('create_committee_meetings'))
    try:
        attendeeOne = Contact.objects.get_or_create(first_name="Fat",
                                                    last_name="Cat",
                                                    role="President",
                                                    phone="+61 555 5555",
                                                    email="fatcat@gmail.com")[0]
        attendeeTwo = Contact.objects.get_or_create(first_name="Humphrey",
                                                    last_name="Bear",
                                                    role="Jester",
                                                    phone="+61 666 5555",
                                                    email="fatbear@gmail.com")[0]

        committee_meeting = CommitteeMeeting.objects.get_or_create(date=django.utils.timezone.now(),
                                                                   location="Kensington")[0]
        committee_meeting.attendees.add(attendeeOne)
        committee_meeting.attendees.add(attendeeTwo)

        count = randrange(15)
        for counter, species in enumerate(Species.objects.all()):
            committee_meeting.species.add(species)
            if counter == count:
                break

        if save_to_database:
            attendeeOne.save()
            attendeeTwo.save()
            committee_meeting.save()
        print('--------------     Completed {}'.format('create_committee_meetings'))

    except Exception as e:
        print("FAILED create_committee_meetings failed: ", e)


def create_change_codes():
    print('--------------Starting {}'.format('create_change_codes'))
    try:
        change_code = ConservationChangeCode.objects.get_or_create(code="Test_Code")[0]
        
        if save_to_database:
            change_code.save()
        print('--------------     Completed {}'.format('create_change_codes'))
        create_group_types()
    except Exception as e:
        print("FAILED create_change_codes failed: ", e)


def create_region_district():
    print('--------------Starting {}'.format('create_region_district'))
    try:
        region = Region.objects.get_or_create(name=REGION_SOUTH_WEST)[0]
        district = District.objects.get_or_create(name=DISTRICT_SWAN_COASTAL, region=region)[0]
        
        if save_to_database:
            region.save()
            district.save()
        print('--------------     Completed {}'.format('create_region_district'))
        create_name_authority()
    except Exception as e:
        print("FAILED create_region_district failed: ", e)

def create_group_types():
    print('--------------Starting {}'.format('create_group_types'))
    try:
        flora_group_type = GroupType.objects.get_or_create(name=GroupType.GROUP_TYPES[0][0])[0]
        fauna_group_type = GroupType.objects.get_or_create(name=GroupType.GROUP_TYPES[1][0])[0]
        community_group_type = GroupType.objects.get_or_create(name=GroupType.GROUP_TYPES[2][0])[0]

        if save_to_database:
            flora_group_type.save()
            fauna_group_type.save()
            community_group_type.save()
        print('--------------     Completed {}'.format('create_group_types'))
        create_region_district()
    except Exception as e:
        print("FAILED create_group_types failed: ", e)

def create_name_authority():
    print('--------------Starting {}'.format('create_name_authority'))
    try:
        name_authority = NameAuthority.objects.get_or_create(name="WA Museum")

        if save_to_database:
            name_authority[0].save()
        print('--------------     Completed {}'.format('create_name_authority'))
        create_species_fauna()
    except Exception as e:
        print("FAILED create_name_authority failed: ", e)

def create_species_attributes():
    print('--------------Starting {}'.format('create_species_attributes'))
    try:
        for species in Species.objects.all():
            name_reference = "{}_attribute_name_reference".format(species.common_name)
            genetic = "{}_attribute_genetic".format(species.common_name)
            biology = "{}_attribute_biology".format(species.common_name)
            ecology = "{}_attribute_ecology".format(species.common_name)
            fire = "{}_attribute_fire".format(species.common_name)
            disease = "{}_attribute_disease".format(species.common_name)

            species_attributes = SpeciesAttributes.objects.get_or_create(name_reference=name_reference,
                                                                         genetic=genetic,
                                                                         biology=biology,
                                                                         ecology=ecology,
                                                                         fire=fire,
                                                                         disease=disease,
                                                                         species=species)

            if save_to_database:
                species_attributes[0].save()
        print('--------------     Completed {}'.format('create_species_attributes'))
        create_committee_meetings()
    except Exception as e:
        print("FAILED create_species_attributes failed: ", e)

def create_community():
    """
    This will take a fauna data file, create a community entry and save to the database. It will also randomly select a fauna and flora
    to be added to the Community that is created. It is surrounded by try/except
    so if row fails, that row can be recorded as faulty and saved to a file for examination.
    """
    print('--------------Starting {}'.format('create_community'))
    row_failed = False
    failed_rows = []
    data_row = 1

    with open('/home/graeme/workspace/boranga/boranga/components/species_and_communities/tests/communities.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for index, community_row in enumerate(reader):
            if index >= data_row:
                try:
                    community_name = community_row[1]

                    # Don't want it if the name is empty
                    if not community_name: continue

                    change_code = ConservationChangeCode.objects.get_or_create(code="Test_Code")[0]
                    conservation_list = ConservationList.objects.create(code='community_c_list', 
                                                                        label='community_c_list')
                    conservation_category = ConservationCategory.objects.create(code='community_c_category', 
                                                                                label='community_c_category')
                    conservation_criteria = ConservationCriteria.objects.create(code='community_criteria')
                    conservation_status = ConservationStatus.objects.create(change_code=change_code,
                                                                            conservation_list=conservation_list,
                                                                            conservation_category=conservation_category,
                                                                            conservation_criteria=conservation_criteria)

                    community_id = randrange(1000)
                    community_status = "Safe"
                    group_type = GroupType.objects.get(name=GroupType.GROUP_TYPES[2][0])
                    region = Region.objects.get(name=REGION_SOUTH_WEST)
                    district = District.objects.get(name=DISTRICT_SWAN_COASTAL)
                    community = Community.objects.create(group_type=group_type,
                                                         community_name=community_name,
                                                         community_id=community_id,
                                                         community_status=community_status,
                                                         conservation_status=conservation_status,
                                                         region=region,
                                                         district=district,)
                    # Pick a random fauna to add to community
                    fauna_size = len(Species.objects.filter(group_type__name=GroupType.GROUP_TYPES[1][0]))
                    fauna = Species.objects.get(group_type__name=GroupType.GROUP_TYPES[1][0], 
                                                id=randrange(fauna_size))
                    community.species.add(fauna)
                    # Pick a random flora to add to community
                    flora_size = len(Species.objects.filter(group_type__name=GroupType.GROUP_TYPES[0][0]))
                    flora = Species.objects.get(group_type__name=GroupType.GROUP_TYPES[0][0], 
                                                id=randrange(flora_size))
                    community.species.add(flora)
                except Exception as e:
                    row_failed = True
                    failed_rows.append(community_row)
                    print("FAILED create_community failed: ", e)
                    # could write failed_rows to file.

                if not row_failed and save_to_database:
                    community.save()
    print('--------------     Completed {}'.format('create_community'))
    create_species_attributes()

def create_species_fauna():        
    """
    This will take a fauna data file, create a species entry and save to the database. It is surrounded by try/except
    so if row fails, that row can be recorded as faulty and saved to a file for examination.
    """
    print('--------------Starting {}'.format('create_species_fauna'))
    row_failed = False
    failed_rows = []

    data_row = 2
    with open('/home/graeme/workspace/boranga/boranga/components/species_and_communities/tests/fauna.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for index, fauna_row in enumerate(reader):
            if index >= data_row:
                try:
                    change_code = ConservationChangeCode.objects.get_or_create(code="Test_Code")[0]
                    conservation_list = ConservationList.objects.create(code=fauna_row[1], label=fauna_row[1])
                    conservation_category = ConservationCategory.objects.create(code=fauna_row[0].split('.')[2], 
                                                                                label=fauna_row[0].split('.')[2])
                    if conservation_category is None:
                        conservation_category = ConservationCategory.objects.create(code="code", 
                                                                                    label="label")
                    conservation_criteria = ConservationCriteria.objects.create(code=fauna_row[4])
                    conservation_status = ConservationStatus.objects.create(change_code=change_code,
                                                                            conservation_list=conservation_list,
                                                                            conservation_category=conservation_category,
                                                                            conservation_criteria=conservation_criteria)

                    taxon = fauna_row[6]
                    taxon_id = randrange(5000)
                    family = fauna_row[6]
                    genus = fauna_row[6]
                    phylogenetic_group = fauna_row[6]
                    name_authority = NameAuthority.objects.get_or_create(name="WA Museum")[0]
                    taxonomy = Taxonomy.objects.create(taxon=taxon,
                                                       taxon_id=taxon_id,
                                                       family=family,
                                                       genus=genus,
                                                       phylogenetic_group=phylogenetic_group,
                                                       name_authority=name_authority,)
                    region = Region.objects.get(name=REGION_SOUTH_WEST)
                    district = District.objects.get(name=DISTRICT_SWAN_COASTAL)
                    group_type = GroupType.objects.get(name=GroupType.GROUP_TYPES[1][0])
                    fauna = Species.objects.create(common_name=fauna_row[7],
                                                   group_type=group_type,
                                                   scientific_name = fauna_row[6],
                                                   conservation_status = conservation_status,
                                                   region=region,
                                                   district=district,
                                                   image = "path/to/fauna.jpg",
                                                   taxonomy = taxonomy)

                    document = "{}.pdf".format(fauna_row[5])
                    document_description = "{}.pdf".format(fauna_row[5])

                    document_category = DocumentCategory.objects.create(name="Fauna_C1")
                    species_document = SpeciesDocument.objects.create(document=document,
                                                                      document_description=document_description,
                                                                      document_category=document_category,
                                                                      species=fauna)


                    threat_category = ThreatCategory.objects.get_or_create(name="Killer Robots")[0]
                    threat_description = fauna_row[3]
                    comment = "{} is fauna".format(fauna_row[8])
                    document = "{}.pdf".format(fauna_row[5])
                    source = "WA Museum"
                    conservation_threat = ConservationThreat(species=fauna,
                                                             threat_category=threat_category,
                                                             threat_description=threat_description,
                                                             comment=comment,
                                                             document=document,
                                                             source=source,)

                    _type = "Regular"
                    comment = "{} is a fauna plan.".format(fauna_row[8])
                    source = "WA Museum"
                    conservation_plans = ConservationPlan.objects.create(region=region,
                                                                         district=district,
                                                                         type=_type,
                                                                         comment=comment,
                                                                         source=source,)
                    conservation_plans.species.add(fauna)

                    department_file_numbers = fauna_row[3]
                    community_original_area = randrange(10000)
                    community_original_area_accuracy = randrange(1000)
                    number_of_occurrences = randrange(100)
                    extent_of_occurrences = randrange(100)
                    area_of_occupancy = randrange(100)
                    number_of_iucn_locations = randrange(100)
                    community_original_area_reference = fauna_row[7]
                    distribution = Distribution.objects.create(department_file_numbers=department_file_numbers,
                                                            community_original_area=community_original_area,
                                                            community_original_area_accuracy=community_original_area_accuracy,
                                                            number_of_occurrences=number_of_occurrences,
                                                            extent_of_occurrences=extent_of_occurrences,
                                                            area_of_occupancy=area_of_occupancy,
                                                            number_of_iucn_locations=number_of_iucn_locations,
                                                            community_original_area_reference=community_original_area_reference,
                                                            species=fauna)

                    general_management_advice = "{} is a fauna gen management advice.".format(fauna_row[8])
                    ecological_attributes = "{} is a fauna eco attribute.".format(fauna_row[8])
                    biological_attributes = "{} is a fauna bio attribute.".format(fauna_row[8])
                    specific_survey_advice = "{} is a fauna survey advice.".format(fauna_row[8])
                    comments = "{} is a fauna comment.".format(fauna_row[8])
                    conservation_attributes = ConservationAttributes.objects.create(general_management_advice=general_management_advice,
                                                                                    ecological_attributes=ecological_attributes,
                                                                                    biological_attributes=biological_attributes,
                                                                                    specific_survey_advice=specific_survey_advice,
                                                                                    comments=comments,
                                                                                    species=fauna)
                except Exception as e:
                    row_failed = True
                    failed_rows.append(fauna_row)
                    print('FAILED create_species_fauna - {}: {}'.format(e, fauna_row))

                if not row_failed and save_to_database:
                    conservation_list.save()
                    conservation_category.save()
                    conservation_criteria.save()
                    conservation_status.save()
                    taxonomy.save()
                    fauna.save()
                    document_category.save()
                    species_document.save()
                    conservation_threat.save()
                    conservation_plans.save()
                    distribution.save()
                    conservation_attributes.save()

    print('--------------     Completed {}'.format('create_species_fauna'))
    create_species_flora()

def create_species_flora():
    print('--------------Starting {}'.format('create_species_flora'))
    """
    This will take a flora data file, create a species entry and save to the database. It is surrounded by try/except
    so if row fails, that row can be recorded as faulty and saved to a file for examination.
    """
    row_failed = False
    failed_rows = []

    data_row = 2
    with open('/home/graeme/workspace/boranga/boranga/components/species_and_communities/tests/flora.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for index, flora_row in enumerate(reader):
            if index >= data_row:
                with suppress(IndexError):
                    try:
                        change_code = ConservationChangeCode.objects.get_or_create(code="Test_Code")[0]
                        conservation_list = ConservationList.objects.create(code=flora_row[1], label=flora_row[1])
                        conservation_category = ConservationCategory.objects.create(code=flora_row[0].split('.')[2], 
                                                                                    label=flora_row[0].split('.')[2])
                        if conservation_category is None:
                            conservation_category = ConservationCategory.objects.create(code="code", 
                                                                                        label="label")
                        conservation_criteria = ConservationCriteria.objects.create(code=flora_row[4])
                        conservation_status = ConservationStatus.objects.create(change_code=change_code,
                                                                                conservation_list=conservation_list,
                                                                                conservation_category=conservation_category,
                                                                                conservation_criteria=conservation_criteria)

                        taxon = flora_row[6]
                        taxon_id = randrange(5000)
                        family = flora_row[6]
                        genus = flora_row[6]
                        phylogenetic_group = flora_row[6]
                        name_authority = NameAuthority.objects.get_or_create(name="Herbarium")[0]
                        taxonomy = Taxonomy.objects.create(taxon=taxon,
                                                        taxon_id=taxon_id,
                                                        family=family,
                                                        genus=genus,
                                                        phylogenetic_group=phylogenetic_group,
                                                        name_authority=name_authority,)

                        group_type = GroupType.objects.get(name=GroupType.GROUP_TYPES[0][0])
                        region = Region.objects.get(name=REGION_SOUTH_WEST)
                        district = District.objects.get(name=DISTRICT_SWAN_COASTAL)                    
                        flora = Species.objects.create(common_name=flora_row[7],
                                                    group_type=group_type,
                                                    scientific_name = flora_row[6],
                                                    conservation_status = conservation_status,
                                                    region=region,
                                                    district=district,
                                                    image = "path/to/flora.jpg",
                                                    taxonomy = taxonomy
                        )

                        document = "{}.pdf".format(flora_row[5])
                        document_description = "{}.pdf".format(flora_row[5])
                        document_category = DocumentCategory.objects.create(name="Flora_C1")
                        species_document = SpeciesDocument.objects.create(document=document,
                                                                          document_description=document_description,
                                                                          document_category=document_category,
                                                                          species=flora)

                        threat_category = ThreatCategory.objects.get_or_create(name="Killer Robots")[0]
                        threat_description = flora_row[3]
                        comment = "{} is fauna".format(flora_row[8])
                        document = "{}.pdf".format(flora_row[5])
                        source = "WA Museum"
                        conservation_threat = ConservationThreat(species=flora,
                                                                threat_category=threat_category,
                                                                threat_description=threat_description,
                                                                comment=comment,
                                                                document=document,
                                                                source=source,)

                        _type = "Regular"
                        comment = "{} is a fauna plan.".format(flora_row[8])
                        source = "WA Museum"
                        conservation_plans = ConservationPlan.objects.create(region=region,
                                                                            district=district,
                                                                            type=_type,
                                                                            comment=comment,
                                                                            source=source,)
                        conservation_plans.species.add(flora)

                        department_file_numbers = flora_row[3]
                        community_original_area = randrange(10000)
                        community_original_area_accuracy = randrange(1000)
                        number_of_occurrences = randrange(100)
                        extent_of_occurrences = randrange(100)
                        area_of_occupancy = randrange(100)
                        number_of_iucn_locations = randrange(100)
                        community_original_area_reference = flora_row[7]
                        distribution = Distribution.objects.create(department_file_numbers=department_file_numbers,
                                                                community_original_area=community_original_area,
                                                                community_original_area_accuracy=community_original_area_accuracy,
                                                                number_of_occurrences=number_of_occurrences,
                                                                extent_of_occurrences=extent_of_occurrences,
                                                                area_of_occupancy=area_of_occupancy,
                                                                number_of_iucn_locations=number_of_iucn_locations,
                                                                community_original_area_reference=community_original_area_reference,
                                                                species=flora)

                        general_management_advice = "{} is a flora gen management advice.".format(flora_row[8])
                        ecological_attributes = "{} is a flora eco attribute.".format(flora_row[8])
                        biological_attributes = "{} is a flora bio attribute.".format(flora_row[8])
                        specific_survey_advice = "{} is a flora survey advice.".format(flora_row[8])
                        comments = "{} is a flora comment.".format(flora_row[8])
                        conservation_attributes = ConservationAttributes.objects.create(general_management_advice=general_management_advice,
                                                                                        ecological_attributes=ecological_attributes,
                                                                                        biological_attributes=biological_attributes,
                                                                                        specific_survey_advice=specific_survey_advice,
                                                                                        comments=comments,
                                                                                        species=flora)
                    except Exception as e:
                        row_failed = True
                        failed_rows.append(flora_row)
                        print('create_species_flora - {}: {}'.format(e, flora_row))
                        # could write failed_rows to file.

                if not row_failed and save_to_database:
                    conservation_list.save()
                    conservation_category.save()
                    conservation_criteria.save()
                    conservation_status.save()
                    taxonomy.save()
                    flora.save()
                    species_document.save()
                    conservation_threat.save()
                    conservation_plans.save()
                    distribution.save()
    print('--------------     Completed {}'.format('create_species_flora'))
    create_community()

