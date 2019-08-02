"""
This module extracts institution information from the HESA
XML dataset and writes it, in JSON format, to Cosmos DB.

Currently, if expected data is missing, we let the exception
bubble up.
"""
import datetime
import inspect
import logging
import os
import sys
import xml.etree.ElementTree as ET

import xmltodict

# TODO investigate setting PATH in Azure so can remove this
CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from EtlPipelineBlobTrigger.course_docs import get_code_label_entry
from CreateInstBlobTrigger.locations import Locations
from SharedCode.utils import (
    get_collection_link,
    get_cosmos_client,
    get_ukrlp_lookups,
    get_uuid,
    get_english_welsh_item,
)
import course_lookup_tables as lookup


class InstitutionDocs:
    def __init__(self, xml_string):
        self.ukrlp_lookups = get_ukrlp_lookups()
        self.root = ET.fromstring(xml_string)
        self.location_lookup = Locations(self.root)

    def get_course(self, raw_course_data):
        # TODO complete and call this
        course = {}
        distance_learning = get_code_label_entry(
            raw_course_data, lookup.distance_learning_lookup, "distance"
        )
        if distance_learning:
            course["distance_learning"] = distance_learning
        if "honours" in raw_course_data:
            course["honours_award_provision"] = raw_course_data["honours"]
        course["kis_course_id"] = raw_course_data["kiscourseid"]
        mode = get_code_label_entry(raw_course_data, lookup.mode, "kismode")
        if mode:
            course["mode"] = mode
        return course

    def get_contact_details(self, ukprn):
        if ukprn not in self.ukrlp_lookups:
            return {"No contact details available": f"UKPRN: {ukprn}"}
        return self.ukrlp_lookups[ukprn]["contact_details"]

    def get_ukprn_name(self, ukprn):
        if ukprn not in self.ukrlp_lookups:
            return {"No name available": f"UKPRN: {ukprn}"}
        return self.ukrlp_lookups[ukprn]["ukprn_name"]

    def get_institution_element(self, institution):
        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]
        institution_element = {}
        if "APROutcome" in raw_inst_data:
            institution_element["apr_outcome"] = raw_inst_data["APROutcome"]
        institution_element["contact_details"] = self.get_contact_details(
            raw_inst_data["PUBUKPRN"]
        )
        institution_element["pub_ukprn_name"] = self.get_ukprn_name(
            raw_inst_data["PUBUKPRN"]
        )
        institution_element["pub_ukprn"] = raw_inst_data["PUBUKPRN"]
        institution_element["pub_ukprn_country"] = get_country(
            raw_inst_data["PUBUKPRNCOUNTRY"]
        )
        if "TEFOutcome" in raw_inst_data:
            institution_element["tef_outcome"] = raw_inst_data["TEFOutcome"]
        institution_element[
            "total_number_of_courses"
        ] = get_total_number_of_courses(institution)
        institution_element["ukprn_name"] = self.get_ukprn_name(
            raw_inst_data["UKPRN"]
        )
        institution_element["ukprn"] = raw_inst_data["UKPRN"]
        institution_element["pub_ukprn_country"] = get_country(
            raw_inst_data["COUNTRY"]
        )
        return institution_element

    def get_institution_doc(self, institution):
        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]
        institution_doc = {}
        institution_doc["_id"] = get_uuid()
        institution_doc["created_at"] = datetime.datetime.utcnow().isoformat()
        institution_doc["version"] = 1
        institution_doc["institution_id"] = raw_inst_data["PUBUKPRN"]
        institution_doc["institution"] = self.get_institution_element(
            institution
        )
        return institution_doc

    def create_institution_docs(self):
        """Parse HESA XML and create JSON institution docs in Cosmos DB."""

        # TODO Investigate writing docs to CosmosDB in bulk to speed things up.
        cosmosdb_client = get_cosmos_client()

        collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbInstitutionsCollectionId"
        )

        institution_count = 0
        for institution in self.root.iter("INSTITUTION"):
            institution_count += 1
            institution_doc = self.get_institution_doc(institution)
            cosmosdb_client.CreateItem(collection_link, institution_doc)
        logging.info(f"Processed {institution_count} institutions")


def get_country(code):
    country = {}
    country["code"] = code
    country["name"] = lookup.country_code[code]
    return country


def get_student_union(location):
    student_union = {}
    link = get_english_welsh_item("SUURL", location)
    if link:
        student_union["link"] = link
    name = get_english_welsh_item("LOCNAME", location)
    if name:
        student_union["name"] = name
    return student_union


def get_student_unions(locations_lookup, institution):
    raw_inst_data = xmltodict.parse(ET.tostring(institution))["INSTITUTION"]
    pubukprn = raw_inst_data["PUBUKPRN"]
    student_unions = []
    for course in institution.findall("KISCOURSE"):
        for course_location in course.findall("COURSELOCATION"):
            student_union = {}
            raw_course_location = xmltodict.parse(ET.tostring(course_location))
            if "LOCID" not in raw_course_location:
                # LOCID is not mandatory in COURSELOCATION
                continue
            locid = raw_course_location["LOCID"]
            location_lookup_key = f"{pubukprn}{locid}"
            location = locations_lookup.get_location(location_lookup_key)
            if location:
                student_union = get_student_union(location)
                if student_union:
                    student_unions.append(student_union)
    return student_unions


def get_total_number_of_courses(institution):
    return len(institution.findall("KISCOURSE"))
