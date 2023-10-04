"""
This module extracts institution information from the HESA
XML dataset and writes it, in JSON format, to Cosmos DB.

Currently, if expected data is missing, we let the exception
bubble up.
"""

import csv
import datetime
import inspect
import logging
import os
import re
import sys
import time

import defusedxml.ElementTree as ET
import xmltodict

from CreateInst.locations import Locations
from EtlPipeline import course_lookup_tables as lookup
from SharedCode import exceptions
from SharedCode.blob_helper import BlobHelper
from SharedCode.utils import get_collection_link
from SharedCode.utils import get_cosmos_client
from SharedCode.utils import get_english_welsh_item
from SharedCode.utils import get_uuid
from SharedCode.utils import normalise_url
from SharedCode.utils import sanitise_address_string

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)


def validate_headers(header: str, xml: str):
    if xml.find(header):
        return True
    logging.error(f"{header} not found")
    return False


def add_tef_data(raw_inst_data):
    return dict(
        report_ukprn=raw_inst_data["REPORT_UKPRN"],
        overall_rating=raw_inst_data["OVERALL_RATING"],
        student_experience_rating=raw_inst_data["STUDENT_EXPERIENCE_RATING"],
        student_outcomes_rating=raw_inst_data["STUDENT_OUTCOMES_RATING"],
        outcome_url=raw_inst_data["OUTCOME_URL"]
    )


def validate_column_headers(header_row):
    logging.info(f"Validating header row, headers: {header_row}")
    header_list = header_row.split(",")

    try:
        valid = True
        if header_list[0] != "ukprn":
            logging.info(f"got in ukprn: {header_list[0]}")
            valid = False

        # WELSH NAME COMES FROM OFS
        if header_list[1] != "welsh_name":
            logging.info(f"got in welsh_name: {header_list[1]}")
            valid = False
    except IndexError:
        logging.exception(f"index out of range\nheader_row:{header_row}")
        valid = False

    return valid


def get_welsh_uni_names():
    use_local_test_XML_file = os.environ.get('UseLocalTestXMLFile')

    if use_local_test_XML_file:
        et_test = ET.parse(os.environ["LocalTestXMLFile"])
        uprnEl = et_test.find('UKPRN')
        mock_xml_source_file = open(os.environ["LocalTestXMLFile"], "r")
        csv_string = mock_xml_source_file.read()
        welsh_uni_names = ["welsh one", "welsh two"]
        return welsh_uni_names
    else:
        storage_container_name = os.environ["AzureStorageWelshUnisContainerName"]
        storage_blob_name = os.environ["AzureStorageWelshUnisBlobName"]
        blob_helper = BlobHelper()
        csv_string = blob_helper.get_str_file(storage_container_name, storage_blob_name)

        _welsh_uni_names = []
        if csv_string:
            rows = csv_string.splitlines()

            if not validate_column_headers(rows[0]):
                logging.error(
                    "file headers are incorrect, expecting the following: code, welsh_label"
                )
                raise exceptions.StopEtlPipelineErrorException

            _welsh_uni_names = rows

        return _welsh_uni_names


def get_white_list():
    file_path = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )
    with open(
            os.path.join(file_path, "institution_whitelist.txt")
    ) as f:
        institutions_whitelist = f.readlines()
        return [institution.strip() for institution in institutions_whitelist]


class InstitutionProviderNameHandler:
    def __init__(self, white_list, welsh_uni_names):
        self.white_list = white_list
        self.welsh_uni_names = welsh_uni_names

    @staticmethod
    def title_case(s):
        s = re.sub(
            r"[A-Za-z]+('[A-Za-z]+)?",
            lambda word: word.group(0).capitalize(),
            s
        )

        exclusions = ["an", "and", "for", "in", "of", "the"]
        word_list = s.split()
        result = [word_list[0]]
        for word in word_list[1:]:
            result.append(word.lower() if word.lower() in exclusions else word)

        s = " ".join(result)

        return s

    def get_welsh_uni_name(self, pub_ukprn, provider_name) -> str:
        rows = csv.reader(self.welsh_uni_names)
        for row in rows:
            if row[0] == pub_ukprn:
                logging.info(f"Found welsh name for {pub_ukprn}")
                return row[1]
        return provider_name

    def should_edit_title(self, title):
        if title not in self.white_list:
            return True
        return False

    def presentable(self, provider_name):
        if self.should_edit_title(provider_name):
            provider_name = self.title_case(provider_name)
        return provider_name


class InstitutionDocs:
    def __init__(self, xml_string, version):
        self.version = version
        self.root = ET.fromstring(xml_string)
        self.location_lookup = Locations(self.root)

    def get_institution_element(self, institution):
        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]

        pubukprn = raw_inst_data["PUBUKPRN"]
        institution_element = {}
        if "APROutcome" in raw_inst_data:
            institution_element["apr_outcome"] = raw_inst_data["APROutcome"]

        contact_details = dict(
            address=sanitise_address_string(raw_inst_data.get("PROVADDRESS", "")),
            telephone=raw_inst_data.get("PROVTEL"),
            website=normalise_url(raw_inst_data.get("PROVURL", "")),
        )
        if contact_details:
            institution_element["contact_details"] = contact_details

        institution_element["links"] = {"institution_homepage": contact_details["website"]}

        student_unions = get_student_unions(self.location_lookup, institution)
        if student_unions:
            institution_element["student_unions"] = get_student_unions(
                self.location_lookup, institution
            )

        pn_handler = InstitutionProviderNameHandler(
            white_list=get_white_list(),
            welsh_uni_names=get_welsh_uni_names()
        )

        legal_name = raw_inst_data.get("LEGAL_NAME", "")
        first_trading_name = raw_inst_data.get("FIRST_TRADING_NAME", "")
        other_names = raw_inst_data.get("OTHER_NAMES", "")

        if first_trading_name:
            institution_element["first_trading_name"] = first_trading_name
            institution_element["pub_ukprn_name"] = first_trading_name
            institution_element["pub_ukprn_welsh_name"] = pn_handler.get_welsh_uni_name(
                pub_ukprn=pubukprn,
                provider_name=institution_element["first_trading_name"]
            )
        else:
            institution_element["pub_ukprn_name"] = raw_inst_data.get("LEGAL_NAME", "")
            institution_element["pub_ukprn_welsh_name"] = pn_handler.get_welsh_uni_name(
                pub_ukprn=pubukprn,
                provider_name=institution_element["legal_name"]
            )
        if other_names:
            institution_element["other_names"] = pn_handler.presentable(other_names).split("###")
        institution_element["legal_name"] = pn_handler.presentable(legal_name)

        institution_element["pub_ukprn"] = pubukprn
        institution_element["pub_ukprn_country"] = get_country(
            raw_inst_data["PUBUKPRNCOUNTRY"]
        )
        if "TEFOutcome" in raw_inst_data:
            institution_element["tef_outcome"] = add_tef_data(raw_inst_data["TEFOutcome"])
        if "QAA_Report_Type" in raw_inst_data or "QAA_URL" in raw_inst_data:
            institution_element["qaa_report_type"] = raw_inst_data.get("QAA_Report_Type")
            institution_element["qaa_url"] = raw_inst_data.get("QAA_URL")
        institution_element[
            "total_number_of_courses"
        ] = get_total_number_of_courses(institution)

        institution_element["ukprn_name"] = institution_element["pub_ukprn_name"]
        institution_element["ukprn_welsh_name"] = institution_element["pub_ukprn_welsh_name"]

        institution_element["ukprn"] = raw_inst_data["UKPRN"]
        institution_element["pub_ukprn_country"] = get_country(
            raw_inst_data["COUNTRY"]
        )

        return institution_element

    def get_institution_doc(self, institution):
        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]

        institution_doc = {
            "_id": get_uuid(),
            "created_at": datetime.datetime.utcnow().isoformat(),
            "version": self.version,
            "institution_id": raw_inst_data["PUBUKPRN"],
            "partition_key": str(self.version),
            "institution": self.get_institution_element(
                institution
            ),
        }
        return institution_doc

    def create_institution_docs(self):
        """Parse HESA XML and create JSON institution docs in Cosmos DB."""

        cosmosdb_client = get_cosmos_client()

        collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbInstitutionsCollectionId"
        )

        options = {"partitionKey": str(self.version)}
        sproc_link = collection_link + "/sprocs/bulkImport"

        institution_count = 0
        new_docs = []
        sproc_count = 0
        for institution in self.root.iter("INSTITUTION"):
            institution_count += 1
            sproc_count += 1
            new_docs.append(self.get_institution_doc(institution))
            if sproc_count == 100:
                logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
                cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
                logging.info(f"Successfully loaded another {sproc_count} documents")
                # Reset values
                new_docs = []
                sproc_count = 0
                time.sleep(4)

        if sproc_count > 0:
            logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
            cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
            logging.info(f"Successfully loaded another {sproc_count} documents")

        logging.info(f"Processed {institution_count} institutions")


def get_country(code):
    country = {"code": code, "name": lookup.country_code[code]}
    return country


def get_student_unions(location_lookup, institution):
    pubukprn = xmltodict.parse(ET.tostring(institution))["INSTITUTION"][
        "PUBUKPRN"
    ]
    student_unions = []
    locations_processed = []
    for course in institution.findall("KISCOURSE"):
        for course_location in course.findall("COURSELOCATION"):
            raw_course_location = xmltodict.parse(
                ET.tostring(course_location)
            )["COURSELOCATION"]
            if "LOCID" not in raw_course_location:
                continue
            locid = raw_course_location["LOCID"]
            location_lookup_key = f"{pubukprn}{locid}"
            if location_lookup_key in locations_processed:
                continue
            locations_processed.append(location_lookup_key)
            location = location_lookup.get_location(location_lookup_key)
            if location:
                student_union = get_student_union(location)
                if student_union:
                    student_unions.append(student_union)
    return student_unions


def get_student_union(location):
    student_union = {}
    link = get_english_welsh_item("SUURL", location)
    if link:
        student_union["link"] = link
    name = get_english_welsh_item("LOCNAME", location)
    if name:
        student_union["name"] = name
    return student_union


def get_total_number_of_courses(institution):
    return len(institution.findall("KISCOURSE"))
