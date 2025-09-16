import logging

import requests
from django.conf import settings
from django.core.management.base import BaseCommand

from boranga.components.species_and_communities.email import send_nomos_script_failed
from boranga.components.species_and_communities.models import (
    ClassificationSystem,
    InformalGroup,
    Kingdom,
    Taxonomy,
    TaxonomyRank,
    TaxonPreviousName,
    TaxonVernacular,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch Taxonomy data"

    def handle(self, *args, **options):
        logger.info(f"Running command {__name__}")

        errors = []
        updates = []

        my_url = settings.NOMOS_BLOB_URL

        try:
            logger.info("{}".format("Requesting NOMOS URL"))
            total_count = 0
            taxon_res = requests.get(my_url)
            count = 0
            if taxon_res.status_code == 200:
                logger.info("{}".format("Done Fetching NOMOS data"))
                taxon = taxon_res.json()
                try:
                    for t in taxon:
                        kingdom_id = t["kingdom_id"] if "kingdom_id" in t else None
                        kingdom_fk = None
                        if kingdom_id:
                            try:
                                kingdom_fk = Kingdom.objects.get(kingdom_id=kingdom_id)
                            except Kingdom.DoesNotExist:
                                try:
                                    kingdom_obj, created = (
                                        Kingdom.objects.update_or_create(
                                            kingdom_id=kingdom_id,
                                            defaults={
                                                "kingdom_name": t["kingdom_name"]
                                            },
                                        )
                                    )
                                    kingdom_fk = kingdom_obj
                                except Exception as e:
                                    err_msg = "Create kingdom:"
                                    logger.error(f"{err_msg}\n{str(e)}")
                                    errors.append(str(e))

                        rank_id = t["rank_id"] if "rank_id" in t else None
                        taxon_rank_fk = None
                        if rank_id and kingdom_id:
                            try:
                                taxon_rank_fk = TaxonomyRank.objects.get(
                                    taxon_rank_id=rank_id
                                )
                            except TaxonomyRank.DoesNotExist:
                                try:
                                    rank_obj, created = (
                                        TaxonomyRank.objects.update_or_create(
                                            taxon_rank_id=rank_id,
                                            defaults={
                                                "kingdom_id": kingdom_id,
                                                "kingdom_fk": kingdom_fk,
                                                "rank_name": t["rank_name"],
                                            },
                                        )
                                    )
                                    taxon_rank_fk = rank_obj
                                except Exception as e:
                                    err_msg = "Create taxon rank:"
                                    logger.error(f"{err_msg}\n{str(e)}")
                                    errors.append(str(e))

                        taxon_obj, created = Taxonomy.objects.update_or_create(
                            taxon_name_id=t["taxon_name_id"],
                            defaults={
                                "scientific_name": t["canonical_name"],
                                "kingdom_id": t["kingdom_id"],
                                "kingdom_fk": kingdom_fk,
                                "kingdom_name": t["kingdom_name"],
                                "name_authority": t["author"],
                                "name_comments": t["notes"],
                                "name_currency": t["is_current"],
                                "taxon_rank_id": t["rank_id"],
                                "taxonomy_rank_fk": taxon_rank_fk,
                                "family_id": t["family_id"],
                                "family_name": t["family_canonical_name"],
                                "genera_id": t["genus_id"],
                                "genera_name": t["genus_canonical_name"],
                            },
                        )
                        updates.append(taxon_obj.id)
                        count += 1
                        if count == 10000:
                            total_count += count
                            logger.info(
                                "{} Taxon Records Updated. Continuing...".format(
                                    total_count
                                )
                            )
                            count = 0

                        if taxon_obj:
                            # check if the taxon has vernaculars and then create the TaxonVernacular
                            # records for taxon which will be the "common names"
                            vernaculars = t["vernaculars"] if "vernaculars" in t else ""
                            if vernaculars is not None:
                                try:
                                    # A taxon can have more than one vernaculars(common names)
                                    for v in vernaculars:
                                        obj, created = (
                                            TaxonVernacular.objects.update_or_create(
                                                vernacular_id=v["id"],
                                                defaults={
                                                    "vernacular_name": v["name"],
                                                    "taxonomy": taxon_obj,
                                                    "taxon_name_id": taxon_obj.taxon_name_id,
                                                },
                                            )
                                        )

                                except Exception as e:
                                    err_msg = "Create Taxon Vernacular:"
                                    logger.error(f"{err_msg}\n{str(e)}")
                                    errors.append(str(e))

                            # check if the taxon has classification_system_ids and then create the ClassificationSystem
                            # records for taxon which will be the "phylogenetic groups"
                            classification_systems = (
                                t["class_desc"] if "class_desc" in t else ""
                            )
                            class_system_fk = None
                            if classification_systems is not None:
                                for c in classification_systems:
                                    try:
                                        class_system_fk = (
                                            ClassificationSystem.objects.get(
                                                classification_system_id=c["id"]
                                            )
                                        )
                                    except ClassificationSystem.DoesNotExist:
                                        try:
                                            class_system_obj, created = (
                                                ClassificationSystem.objects.update_or_create(
                                                    classification_system_id=c["id"],
                                                    defaults={
                                                        "class_desc": c["name"],
                                                    },
                                                )
                                            )
                                            class_system_fk = class_system_obj
                                            if class_system_obj:
                                                try:
                                                    obj, created = (
                                                        InformalGroup.objects.update_or_create(
                                                            taxonomy=taxon_obj,
                                                            classification_system_fk=class_system_fk,
                                                            defaults={
                                                                "classification_system_id": class_system_fk.classification_system_id,  # noqa
                                                                "taxon_name_id": taxon_obj.taxon_name_id,
                                                            },
                                                        )
                                                    )
                                                except Exception as e:
                                                    err_msg = "Create informal group:"
                                                    logger.error(f"{err_msg}\n{str(e)}")
                                                    errors.append(str(e))

                                        except Exception as e:
                                            err_msg = (
                                                "Create Taxon Classification Systems:"
                                            )
                                            logger.error(f"{err_msg}\n{str(e)}")
                                            errors.append(str(e))

                            # check if the taxon has previous_names
                            previous_names = (
                                t["previous_names"] if "previous_names" in t else ""
                            )
                            if previous_names is not None:
                                try:
                                    # A taxon can have more than one previous_names
                                    # (at the moment only the latest given in blob)
                                    for p in previous_names:
                                        obj, created = (
                                            TaxonPreviousName.objects.update_or_create(
                                                previous_name_id=p["id"],
                                                defaults={
                                                    "previous_scientific_name": p[
                                                        "name"
                                                    ],
                                                    "taxonomy": taxon_obj,
                                                },
                                            )
                                        )

                                except Exception as e:
                                    err_msg = "Create Taxon Previous Name:"
                                    logger.error(f"{err_msg}\n{str(e)}")
                                    errors.append(str(e))

                    # printing last records out of for loop
                    total_count += count
                    logger.info(f"{total_count} Taxon Records Updated. End")

                except Exception as e:
                    err_msg = "Create Taxon:"
                    logger.error(f"{err_msg}\n{str(e)}")
                    errors.append(str(e))

            else:
                err_msg = "Login failed with status code {}".format(
                    taxon_res.status_code
                )
                logger.error(f"{err_msg}")

        except Exception as e:
            err_msg = "Error at the end"
            logger.error(f"{err_msg}\n{str(e)}")
            errors.append(str(e))

        cmd_name = __name__.split(".")[-1].replace("_", " ").upper()
        err_str = (
            f'<strong style="color: red;">Errors: {len(errors)}</strong>'
            if len(errors) > 0
            else '<strong style="color: green;">Errors: 0</strong>'
        )
        msg = "{} completed. Errors: {}. Total IDs updated: {}.".format(
            cmd_name, err_str, total_count
        )
        logger.info(msg)

        if len(errors) > 0:
            # send email notification
            send_nomos_script_failed(errors)
