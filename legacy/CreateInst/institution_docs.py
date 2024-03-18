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
from typing import Any
from typing import Dict
from typing import List

import defusedxml.ElementTree as ET
import xmltodict

from constants import BLOB_WELSH_UNIS_BLOB_NAME
from constants import BLOB_WELSH_UNIS_CONTAINER_NAME
from constants import XML_LOCAL_TEST_XML_FILE
from constants import XML_USE_LOCAL_TEST_XML_FILE
from legacy.CreateInst.locations import Locations
from legacy.EtlPipeline import course_lookup_tables
from legacy.services import exceptions
from legacy.services.blob import BlobService
from legacy.services.utils import get_collection_link
from legacy.services.utils import get_cosmos_client
from legacy.services.utils import get_english_welsh_item
from legacy.services.utils import get_uuid
from legacy.services.utils import normalise_url
from legacy.services.utils import sanitise_address_string

CURRENT_DIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)


def validate_headers(header: str, xml: str) -> bool:
    if xml.find(header):
        return True
    logging.error(f"{header} not found")
    return False


def add_tef_data(raw_inst_data: Dict[str, Any]) -> Dict[str, Any]:
    return dict(
        report_ukprn=raw_inst_data["REPORT_UKPRN"],
        overall_rating=raw_inst_data["OVERALL_RATING"],
        student_experience_rating=raw_inst_data["STUDENT_EXPERIENCE_RATING"],
        student_outcomes_rating=raw_inst_data["STUDENT_OUTCOMES_RATING"],
        outcome_url=raw_inst_data.get("OUTCOME_URL")
    )


def validate_column_headers(header_row: str) -> bool:
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


def get_welsh_uni_names() -> List[str]:
    if XML_USE_LOCAL_TEST_XML_FILE:
        et_test = ET.parse(XML_LOCAL_TEST_XML_FILE)
        uprnEl = et_test.find('UKPRN')
        mock_xml_source_file = open(XML_LOCAL_TEST_XML_FILE, "r")
        csv_string = mock_xml_source_file.read()
        welsh_uni_names = ["welsh one", "welsh two"]
        return welsh_uni_names
    else:
        blob_service = BlobService()
        csv_string = blob_service.get_str_file(BLOB_WELSH_UNIS_CONTAINER_NAME, BLOB_WELSH_UNIS_BLOB_NAME)

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


def get_white_list() -> List[str]:
    file_path = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )
    with open(
            os.path.join(file_path, "institution_whitelist.txt")
    ) as f:
        institutions_whitelist = f.readlines()
        return [institution.strip() for institution in institutions_whitelist]



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

    def create_institution_docs(self) -> None:
        """Parse HESA XML and create JSON institution docs in Cosmos DB."""

        cosmosdb_client = get_cosmos_client()

        collection_link = get_collection_link(
            "COSMOS_COLLECTION_INSTITUTIONS"
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
                time.sleep(10)

        if sproc_count > 0:
            logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
            cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
            logging.info(f"Successfully loaded another {sproc_count} documents")

        logging.info(f"Processed {institution_count} institutions")


def get_country(code: str) -> Dict[str, Any]:
    country = {"code": code, "name": course_lookup_tables.country_code[code]}
    return country


def get_student_unions(location_lookup: Dict[str, Any], institution: ET) -> List[Dict[str, Any]]:
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


def get_student_union(location: Dict[str, Any]) -> Dict[str, Any]:
    student_union = {}
    link = get_english_welsh_item("SUURL", location)
    if link:
        student_union["link"] = link
    name = get_english_welsh_item("LOCNAME", location)
    if name:
        student_union["name"] = name
    return student_union


def get_total_number_of_courses(institution: ET) -> int:
    return len(institution.findall("KISCOURSE"))
