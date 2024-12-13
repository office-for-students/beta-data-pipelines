"""
This module extracts institution information from the HESA
XML dataset and writes it, in JSON format, to Cosmos DB.

Currently, if expected data is missing, we let the exception
bubble up.
"""

import inspect
import logging
import os
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import defusedxml.ElementTree as ET
import xmltodict

from institution_creation.locations import Locations
from etl_pipeline.lookups import course_lookup_tables
from services import exceptions
from services.utils import get_english_welsh_item

CURRENT_DIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)


def validate_headers(header: str, xml: str) -> bool:
    """
    Takes a header and an XML string, returns True if the header is found in the XML else False

    :param header: Header to search for in XML string
    :type header: str
    :param xml: XML string to search for header
    :type xml: str
    :return: True if the header is found in the XML, else False
    :rtype: bool
    """
    if xml.find(header):
        return True
    logging.error(f"{header} not found")
    return False


def add_tef_data(raw_inst_data: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns a dictionary of TEF data using the given raw institution data

    :param raw_inst_data: Raw institution data used to build TEF dictionary
    :type raw_inst_data: Dict[str, Any]
    :return: Dictionary of TEF data
    :rtype: Union[Dict[str, Any], List[Dict[str, Any]]]
    """
    return dict(
        report_ukprn=raw_inst_data["REPORT_UKPRN"],
        overall_rating=raw_inst_data["OVERALL_RATING"],
        student_experience_rating=raw_inst_data["STUDENT_EXPERIENCE_RATING"],
        student_outcomes_rating=raw_inst_data["STUDENT_OUTCOMES_RATING"],
        outcome_url=raw_inst_data.get("OUTCOME_URL")
    )


def validate_column_headers(header_row: str) -> bool:
    """
    Takes a header row as a string and checks it is of a valid format.

    :param header_row: Header row to validate
    :type header_row: str
    :return: True if the header row is valid, False otherwise
    :rtype: bool
    """
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


def get_welsh_uni_names(csv_string: str) -> List[str]:
    """
    Gets Welsh institution names from a CSV.

    :return: List of Welsh institution names
    :rtype: List[str]
    """

    _welsh_uni_names = []
    if csv_string:
        rows = csv_string.splitlines()
        print("ROWS INSIDE", rows[0])
        if not validate_column_headers(rows[0]):
            logging.error(
                "file headers are incorrect, expecting the following: ukprn, welsh_name"
            )
            raise exceptions.StopEtlPipelineErrorException

        _welsh_uni_names = rows

    return _welsh_uni_names


def get_white_list() -> List[str]:
    """
    Returns a list of white listed institutions

    :return: List of white listed institutions
    :rtype: List[str]
    """
    file_path = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )
    with open(os.path.join(file_path, "institution_whitelist.txt")) as f:
        institutions_whitelist = f.readlines()
        return [institution.strip() for institution in institutions_whitelist]


def get_country(code: str) -> Dict[str, str]:
    """
    Takes a country code and returns a dictionary of country data additionally containing the country name

    :param code: Code used to lookup country
    :type code: str
    :return: Country data
    :rtype: Dict[str, Any]
    """
    country = {
        "code": code,
        "name": course_lookup_tables.country_code[code]
    }
    return country


def get_student_unions(location_lookup: Locations, institution: ET) -> List[Dict[str, Any]]:
    """
    Takes a lookup location object and institution XML element and returns a list of student union data for that
    location and institution.

    :param location_lookup: Location used to retrieve student unions
    :type location_lookup: Locations
    :param institution: Institution used to retrieve student unions
    :type institution: ElementTree
    :return: List of student union data
    :rtype: List[Dict[str, Any]]
    """
    pubukprn = xmltodict.parse(ET.tostring(institution))["INSTITUTION"]["PUBUKPRN"]
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
    """
    Takes location data and returns a dictionary of student union data using the corresponding location

    :param location: Location data to get student union with
    :type location: Dict[str, Any]
    :return: Student union data
    :rtype: Dict[str, Any]
    """
    student_union = {}
    link = get_english_welsh_item("SUURL", location)
    if link:
        student_union["link"] = link
    name = get_english_welsh_item("LOCNAME", location)
    if name:
        student_union["name"] = name
    return student_union


def get_total_number_of_courses(institution: ET) -> int:
    """
    Takes an XML element and returns the total number of kis courses

    :param institution: Institution XML element
    :type institution: ElementTree
    :return: Total number of courses
    :rtype: int
    """
    return len(institution.findall("KISCOURSE"))
