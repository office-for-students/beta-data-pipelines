"""
This module extracts course information from the HESA
XML dataset and writes it in JSON format to Cosmos DB.x
To fit with the recent architecture update this may
require refactoring.

Currently, we handle unexpected exceptions by letting
them bubble up. This should help flush out problems
during development and testing.
"""
import datetime
import inspect
import logging
import os
import sys
import traceback
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import defusedxml.ElementTree as ET
import xmltodict

from constants import BLOB_QUALIFICATIONS_BLOB_NAME
from constants import BLOB_QUALIFICATIONS_CONTAINER_NAME
from constants import COSMOS_COLLECTION_COURSES
from constants import COSMOS_DATABASE_ID
from course_stats import get_earnings_unavail_text
from course_stats import get_stats
from course_subjects import get_subjects
from legacy.EtlPipeline.lookups import course_lookup_tables as lookup
from legacy.EtlPipeline.lookups.accreditations import Accreditations
from legacy.EtlPipeline.lookups.kisaims import KisAims
from legacy.EtlPipeline.lookups.locations import Locations
from legacy.EtlPipeline.lookups.sector_salaries import GOSectorSalaries
from legacy.EtlPipeline.lookups.sector_salaries import LEO3SectorSalaries
from legacy.EtlPipeline.lookups.sector_salaries import LEO5SectorSalaries
from legacy.EtlPipeline.lookups.sector_salaries import SectorSalaries
from legacy.EtlPipeline.mappings.go.institution import GoInstitutionMappings
from legacy.EtlPipeline.mappings.go.salary import GoSalaryMappings
from legacy.EtlPipeline.mappings.go.voice import GoVoiceMappings
from legacy.EtlPipeline.mappings.leo.institution import LeoInstitutionMappings
from legacy.EtlPipeline.mappings.leo.sector import LeoSectorMappings
from legacy.EtlPipeline.stats.shared_utils import SharedUtils
from legacy.services import utils
from legacy.services.utils import get_english_welsh_item
from qualification_enricher import QualificationCourseEnricher
from subject_enricher import SubjectCourseEnricher
from ukrlp_enricher import UkRlpCourseEnricher

CURRENT_DIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)


def load_course_docs(xml_string: str, version: int) -> None:
    """
    Parse HESA XML passed in and create JSON course docs in Cosmos DB.

    :param xml_string: XML dataset to be imported
    :type xml_string: str
    :param version: Version of the dataset to be created
    :type version: int
    :return: None
    """

    cosmos_client = utils.get_cosmos_client()
    cosmos_db_client = cosmos_client.get_database_client(COSMOS_DATABASE_ID)
    cosmos_container_client = cosmos_db_client.get_container_client(COSMOS_COLLECTION_COURSES)

    logging.info(
        "adding ukrlp data into memory ahead of building course documents"
    )
    enricher = UkRlpCourseEnricher(version)

    logging.info(
        "adding subject data into memory ahead of building course documents"
    )
    subject_enricher = SubjectCourseEnricher(version)
    g_subject_enricher = subject_enricher

    logging.info(
        "adding qualification data into memory ahead of building course documents"
    )

    qualification_enricher = QualificationCourseEnricher(BLOB_QUALIFICATIONS_CONTAINER_NAME,
                                                         BLOB_QUALIFICATIONS_BLOB_NAME)

    collection_link = utils.get_collection_link(COSMOS_COLLECTION_COURSES)

    # Import the XML dataset
    root = ET.fromstring(xml_string)

    sproc_link = collection_link + "/sprocs/bulkImport"
    partition_key = str(version)

    new_docs = []
    sproc_count = 0

    # Import accreditations, common, kisaims and location nodes
    accreditations = Accreditations(root)
    kisaims = KisAims(root)
    locations = Locations(root)

    go_sector_salaries = GOSectorSalaries(root)
    leo3_sector_salaries = LEO3SectorSalaries(root)
    leo5_sector_salaries = LEO5SectorSalaries(root)

    course_count = 0
    for institution in root.iter("INSTITUTION"):

        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]

        ukprn = raw_inst_data["UKPRN"]
        logging.info(f"Ingesting course for: ({raw_inst_data['PUBUKPRN']})")
        for course in institution.findall("KISCOURSE"):
            raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
            logging.info(f"COURSE COUNT: {course_count}")
            logging.info(
                f"Ingesting course for: {raw_inst_data['PUBUKPRN']}/{raw_course_data['KISCOURSEID']}/{raw_course_data['KISMODE']}) | start {version}")
            try:
                locids = get_locids(raw_course_data, ukprn)
                course_doc = get_course_doc(
                    accreditations,
                    locations,
                    locids,
                    raw_inst_data,
                    raw_course_data,
                    kisaims,
                    version,
                    go_sector_salaries,
                    leo3_sector_salaries,
                    leo5_sector_salaries,
                    g_subject_enricher
                )
                enricher.enrich_course(course_doc)
                subject_enricher.enrich_course(course_doc)
                qualification_enricher.enrich_course(course_doc)
                new_docs.append(course_doc)
                sproc_count += 1
                logging.info(f"FINISHED COUNT: {course_count}")
                course_count += 1

                if sproc_count >= 5:
                    logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
                    cosmos_container_client.scripts.execute_stored_procedure(sproc_link, params=[new_docs],
                                                                             partition_key=partition_key)
                    logging.info(f"Successfully loaded another {sproc_count} documents")
                    # Reset values
                    new_docs = []
                    sproc_count = 0
            except Exception as e:
                logging.warning(f"FAILED AT COUNT: {course_count}")
                logging.warning(
                    f"FAILED: Ingesting course for: {raw_inst_data['PUBUKPRN']}/{raw_course_data['KISCOURSEID']}/{raw_course_data['KISMODE']}) | end {version}")
                institution_id = raw_inst_data["UKPRN"]
                course_id = raw_course_data["KISCOURSEID"]
                course_mode = raw_course_data["KISMODE"]
                tb = traceback.format_exc()
                exception_text = f"Failed error: {e} when creating the course document for course with institution_id: {institution_id} course_id: {course_id} course_mode: {course_mode} TRACEBACK: {tb}"
                logging.info(exception_text)

    if sproc_count > 0:
        logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
        cosmos_container_client.scripts.execute_stored_procedure(sproc_link, params=[new_docs],
                                                                 partition_key=partition_key)
        logging.info(f"Successfully loaded another {sproc_count} documents")
        # Reset values
        new_docs = []
        sproc_count = 0

    logging.info(f"Processed {course_count} courses")


def get_locids(raw_course_data: Dict[str, Any], ukprn: str) -> List[str]:
    """
    Returns a list of lookup keys for use with the locations class.
    If the course does not contain a location, returns an empty list.

    :param raw_course_data: Course data dictionary
    :type raw_course_data: Dict[str, Any]
    :param ukprn: UKPRN added to each location ID
    :type ukprn: str
    :return: List of location IDs for the course provided
    :rtype: List[str]
    """
    locids = []
    if "COURSELOCATION" not in raw_course_data:
        return locids

    if isinstance(raw_course_data["COURSELOCATION"], list):
        for val in raw_course_data["COURSELOCATION"]:
            locids.append(f"{val.get('LOCID', {})}{ukprn}")
    else:
        locids.append(
            f"{raw_course_data['COURSELOCATION'].get('LOCID', {})}{ukprn}"
        )

    return locids


def get_course_doc(
        accreditations: Accreditations,
        locations: Locations,
        locids: List[str],
        raw_inst_data: Dict[str, Any],
        raw_course_data: Dict[str, Any],
        kisaims: KisAims,
        version: int,
        go_sector_salaries: GOSectorSalaries,
        leo3_sector_salaries: LEO3SectorSalaries,
        leo5_sector_salaries: LEO5SectorSalaries,
        g_subject_enricher: SubjectCourseEnricher
) -> Dict[str, Any]:
    """
    Performs required lookups to construct a comprehensive dictionary with course data. Adds an outer wrapper
    containing information such as date updated.

    :param accreditations: Accreditations object containing a lookup dictionary
    :type accreditations: Accreditations
    :param locations: Locations object containing a lookup dictionary
    :type locations: Locations
    :param locids: List of location IDs as strings
    :type locids: List[str]
    :param raw_inst_data: Raw institution data dictionary
    :type raw_inst_data: Dict[str, Any]
    :param raw_course_data: Raw course data dictionary
    :type raw_course_data: Dict[str, Any]
    :param kisaims: KisAims object containing a lookup dictionary
    :type kisaims: KisAims
    :param version: Version of the dataset to be generated
    :type version: int
    :param go_sector_salaries: GOSectorSalaries object containing a lookup dictionary
    :type go_sector_salaries: GOSectorSalaries
    :param leo3_sector_salaries: LEO3SectorSalaries object containing a lookup dictionary
    :type leo3_sector_salaries: LEO3SectorSalaries
    :param leo5_sector_salaries: LEO5SectorSalaries object containing a lookup dictionary
    :type leo5_sector_salaries: LEO5SectorSalaries
    :param g_subject_enricher: SubjectCourseEnricher object for adding ukprn data
    :type g_subject_enricher: SubjectCourseEnricher
    :return: Dictionary containing all course data
    :rtype: Dict[str, Any]
    """
    outer_wrapper = {
        "_id": utils.get_uuid(),
        "created_at": datetime.datetime.utcnow().isoformat(),
        "updated_at": datetime.datetime.utcnow().isoformat(),
        "version": version,
        "institution_id": raw_inst_data["PUBUKPRN"],
        "course_id": raw_course_data["KISCOURSEID"],
        "course_mode": int(raw_course_data["KISMODE"]),
        "course_level": int(raw_course_data["KISLEVEL"]),
        "partition_key": str(version),
    }

    course = {
        "course_level": int(raw_course_data["KISLEVEL"])
    }

    if "ACCREDITATION" in raw_course_data:
        course["accreditations"] = get_accreditations(
            raw_course_data, accreditations
        )

    if "UKPRNAPPLY" in raw_course_data:
        course["application_provider"] = raw_course_data["UKPRNAPPLY"]
    country = get_country(raw_inst_data)
    if country:
        course["country"] = country
    distance_learning = get_code_label_entry(
        raw_course_data, lookup.distance_learning_lookup, "DISTANCE"
    )
    if distance_learning:
        course["distance_learning"] = distance_learning
    foundation_year = get_code_label_entry(
        raw_course_data, lookup.foundation_year_availability, "FOUNDATION"
    )
    if foundation_year:
        course["foundation_year_availability"] = foundation_year
    if "HONOURS" in raw_course_data:
        course["honours_award_provision"] = int(raw_course_data["HONOURS"])
    course["institution"] = get_institution(raw_inst_data)
    course["kis_course_id"] = raw_course_data["KISCOURSEID"]

    # Handle the institution-level Earnings data.
    # For joint courses, we may get passed an OrderedDict of xxxSAL records.
    # For single-subject courses, not sure if we get passed an OrderedDict of 1 or something else.
    go_inst_xml_nodes = raw_course_data["GOSALARY"]
    if go_inst_xml_nodes:
        course["go_salary_inst"] = get_go_inst_json(go_inst_xml_nodes,
                                                    subject_enricher=g_subject_enricher)  # Returns an array.

    leo3_inst_xml_nodes = raw_course_data["LEO3"]
    if leo3_inst_xml_nodes:
        course["leo3_inst"] = get_leo3_inst_json(leo3_inst_xml_nodes, subject_enricher=g_subject_enricher)

    leo5_inst_xml_nodes = raw_course_data["LEO5"]
    if leo5_inst_xml_nodes:
        course["leo5_inst"] = get_leo5_inst_json(leo5_inst_xml_nodes, subject_enricher=g_subject_enricher)

    go_voice_work_xml_nodes = raw_course_data["GOVOICEWORK"]
    if go_voice_work_xml_nodes:
        course["go_voice_work"] = get_go_voice_work_json(go_voice_work_xml_nodes, subject_enricher=g_subject_enricher)

    length_of_course = get_code_label_entry(
        raw_course_data, lookup.length_of_course, "NUMSTAGE"
    )
    if length_of_course:
        course["length_of_course"] = length_of_course
    links = get_links(raw_inst_data, raw_course_data)
    if links:
        course["links"] = links
    location_items = get_location_items(
        locations, locids, raw_course_data, raw_inst_data["PUBUKPRN"]
    )
    if location_items:
        course["locations"] = location_items
    mode = get_code_label_entry(raw_course_data, lookup.mode, "KISMODE")
    if mode:
        course["mode"] = mode
    nhs_funded = get_code_label_entry(
        raw_course_data, lookup.nhs_funded, "NHS"
    )
    if nhs_funded:
        course["nhs_funded"] = nhs_funded
    qualification = get_qualification(raw_course_data, kisaims)
    if qualification:
        course["qualification"] = qualification
    sandwich_year = get_code_label_entry(
        raw_course_data, lookup.sandwich_year, "SANDWICH"
    )
    if sandwich_year:
        course["sandwich_year"] = sandwich_year

    course["subjects"] = get_subjects(raw_course_data)

    title = get_english_welsh_item("TITLE", raw_course_data)
    kis_aim_code = raw_course_data["KISAIMCODE"]  # KISAIMCODE is guaranteed to exist and have a non-null value.
    kis_aim_label = get_kis_aim_label(kis_aim_code, kisaims)
    if title and title['english'] and kis_aim_label and title['english'] == kis_aim_label:
        course[
            "title"] = title  # TODO: change this statement as appropriate, not sure how yet - awaiting OfS requirement.
    elif title:
        course["title"] = title
    else:
        course["title"] = {"english": kis_aim_label}

    if "UCASPROGID" in raw_course_data:
        course["ucas_programme_id"] = raw_course_data["UCASPROGID"]
    year_abroad = get_code_label_entry(
        raw_course_data, lookup.year_abroad, "YEARABROAD"
    )
    if year_abroad:
        course["year_abroad"] = get_code_label_entry(
            raw_course_data, lookup.year_abroad, "YEARABROAD"
        )

    course["statistics"] = get_stats(raw_course_data)

    # Extract the appropriate sector-level earnings data for the current course.
    go_sector_json_array = get_go_sector_json(
        course["go_salary_inst"],
        course["leo3_inst"],
        course["leo5_inst"],
        go_sector_salaries,
        outer_wrapper["course_mode"],
        outer_wrapper["course_level"],
        subject_enricher=g_subject_enricher
    )
    if go_sector_json_array:
        course["go_salary_sector"] = go_sector_json_array

    leo3_sector_json_array = get_leo3_sector_json(
        course["leo3_inst"],
        course["go_salary_inst"],
        course["leo5_inst"],
        leo3_sector_salaries,
        outer_wrapper["course_mode"],
        outer_wrapper["course_level"],
        subject_enricher=g_subject_enricher
    )
    if leo3_sector_json_array:
        course["leo3_salary_sector"] = leo3_sector_json_array

    leo5_sector_json_array = get_leo5_sector_json(
        course["leo5_inst"],
        course["go_salary_inst"],
        course["leo3_inst"],
        leo5_sector_salaries,
        outer_wrapper["course_mode"],
        outer_wrapper["course_level"],
        subject_enricher=g_subject_enricher
    )
    if leo5_sector_json_array:
        course["leo5_salary_sector"] = leo5_sector_json_array

    outer_wrapper["course"] = course
    return outer_wrapper


def get_accreditations(raw_course_data: Dict[str, Any], acc_lookup: Accreditations) -> List[Dict[str, Any]]:
    """
    Takes a course data dictionary and returns a list of associated accreditations as dictionaries.

    :param raw_course_data: Course data dictionary to extract accreditations from
    :type raw_course_data: Dict[str, Any]
    :param acc_lookup: Accreditations object containing a lookup dictionary
    :type acc_lookup: Accreditations
    :return: List of accreditations dictionaries
    :rtype: List[Dict[str, Any]]
    """
    acc_list = []
    raw_xml_list = SharedUtils.get_raw_list(raw_course_data, "ACCREDITATION")

    for xml_elem in raw_xml_list:
        json_elem = {}

        if "ACCTYPE" in xml_elem:
            json_elem["type"] = xml_elem["ACCTYPE"]
            accreditations = acc_lookup.get_accreditation_data_for_key(
                xml_elem["ACCTYPE"]
            )

            if "ACCURL" in accreditations:
                json_elem["accreditor_url"] = accreditations["ACCURL"]

            text = get_english_welsh_item("ACCTEXT", accreditations)
            json_elem["text"] = text

        if "ACCDEPENDURL" in xml_elem or "ACCDEPENDURLW" in xml_elem:
            urls = get_english_welsh_item("ACCDEPENDURL", xml_elem)
            json_elem["url"] = urls

        dependent_on = get_code_label_entry(
            xml_elem, lookup.accreditation_code, "ACCDEPEND"
        )
        if dependent_on:
            json_elem["dependent_on"] = dependent_on

        acc_list.append(json_elem)

    return acc_list


def get_country(raw_inst_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes institution data and returns a dictionary with that institution's country data.
    Returns an empty dictionary if the institution does not have a country element.

    :param raw_inst_data: Institution data dictionary
    :type raw_inst_data: Dict[str, Any]
    :return: Dictionary containing country data, or an empty dictionary if the institution has no country
    :rtype: Dict[str, Any]
    """
    country = {}
    if "COUNTRY" in raw_inst_data:
        code = raw_inst_data["COUNTRY"]
        country["code"] = code
        country["name"] = lookup.country_code[code]
    return country


def get_go_inst_json(raw_go_inst_data: Dict[str, Any], subject_enricher: SubjectCourseEnricher) -> List[Dict[str, str]]:
    """
    Takes GO institution data and returns as a list of dictionaries.
    If no data is supplied, returns a dictionary with unavailable texts.

    :param raw_go_inst_data: Data dictionary of LEO5 institution data
    :type raw_go_inst_data: Dict[str, Any]
    :param subject_enricher: SubjectCourseEnricher object to add ukprn data
    :type subject_enricher: SubjectCourseEnricher
    :return: List of JSON dictionaries containing GO institution data, or a list with a dictionary containing
    unavailable texts if no data is supplied
    :rtype: List[Dict[str, Any]]
    """
    if raw_go_inst_data:
        if isinstance(raw_go_inst_data, dict):
            raw_go_inst_data = [raw_go_inst_data]

        mapper = GoInstitutionMappings(
            mapping_id="GO",
            subject_enricher=subject_enricher
        )

        return mapper.map_xml_to_json_array(
            xml_as_array=raw_go_inst_data,
        )
    else:
        unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "go", "1")
        go_salary = {
            "unavail_text_english": unavail_text_english,
            "unavail_text_welsh": unavail_text_welsh
        }

    return [go_salary]


def get_leo3_inst_json(
        raw_leo3_inst_data: Dict[str, Any],
        subject_enricher: SubjectCourseEnricher
) -> List[Dict[str, str]]:
    """
    Takes LEO3 institution data and returns as a list of dictionaries.
    If no data is supplied, returns a dictionary with unavailable texts.

    :param raw_leo3_inst_data: Data dictionary of LEO5 institution data
    :type raw_leo3_inst_data: Dict[str, Any]
    :param subject_enricher: SubjectCourseEnricher object to add ukprn data
    :type subject_enricher: SubjectCourseEnricher
    :return: List of JSON dictionaries containing LEO3 institution data, or a list with a dictionary containing
    unavailable texts if no data is supplied
    :rtype: List[Dict[str, Any]]
    """
    if raw_leo3_inst_data:
        if isinstance(raw_leo3_inst_data, dict):
            raw_leo3_inst_data = [raw_leo3_inst_data]

        mapper = LeoInstitutionMappings(
            mapping_id="LEO3",
            subject_enricher=subject_enricher
        )
        return mapper.map_xml_to_json_array(
            xml_as_array=raw_leo3_inst_data,
        )
    else:
        unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", "1")
        leo3 = {"unavail_text_english": unavail_text_english, "unavail_text_welsh": unavail_text_welsh}
        return [leo3]


def get_leo5_inst_json(
        raw_leo5_inst_data: Dict[str, Any],
        subject_enricher: SubjectCourseEnricher
) -> List[Dict[str, Any]]:
    """
    Takes LEO5 institution data and returns as a list of dictionaries.
    If no data is supplied, returns a dictionary with unavailable texts.

    :param raw_leo5_inst_data: Data dictionary of LEO5 institution data
    :type raw_leo5_inst_data: Dict[str, Any]
    :param subject_enricher: SubjectCourseEnricher object to add ukprn data
    :type subject_enricher: SubjectCourseEnricher
    :return: List of JSON dictionaries containing LEO5 institution data, or a list with a dictionary containing
    unavailable texts if no data is supplied
    :rtype: List[Dict[str, Any]]
    """
    if raw_leo5_inst_data:
        if isinstance(raw_leo5_inst_data, dict):
            raw_leo5_inst_data = [raw_leo5_inst_data]

        mapper = LeoInstitutionMappings("LEO5", subject_enricher=subject_enricher)
        return mapper.map_xml_to_json_array(
            xml_as_array=raw_leo5_inst_data,
        )

    else:
        unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", "1")
        leo5 = {"unavail_text_english": unavail_text_english, "unavail_text_welsh": unavail_text_welsh}

        return [leo5]


def get_go_voice_work_json(
        raw_go_voice_work_data: Dict[str, Any],
        subject_enricher: SubjectCourseEnricher
) -> Union[List[Dict[str, Any]], None]:
    """
    Takes go voice data as a dictionary and returns the data as a list of dictionaries.
    Returns None if no data is supplied

    :param raw_go_voice_work_data: GO voice data as a dictionary
    :type raw_go_voice_work_data: Dict[str, Any]
    :param subject_enricher: SubjectCourseEnricher object to add ukprn data
    :type subject_enricher: SubjectCourseEnricher
    :return: List of GO voice data dictionaries, or None if no data is supplied
    :rtype: Union[List[Dict[str, Any]], None]
    """
    if raw_go_voice_work_data:
        if isinstance(raw_go_voice_work_data, dict):
            raw_go_voice_work_data = [raw_go_voice_work_data]
        mapper = GoVoiceMappings(
            mapping_id="GO",
            subject_enricher=subject_enricher
        )
        return mapper.map_xml_to_json_array(
            xml_as_array=raw_go_voice_work_data,
        )


def get_code_label_entry(
        lookup_table_raw_xml: Dict[str, Any],
        lookup_table_local: Dict[int, str],
        key: str
) -> Dict[str, Any]:
    """
    Takes a code from the XML lookup table and constructs a dictionary pairing them with corresponding codes
    from the local lookup table, which is then returned.

    :param lookup_table_raw_xml: XML lookup table
    :type lookup_table_raw_xml: Dict[str, Any]
    :param lookup_table_local: Local lookup table
    :type lookup_table_local: Dict[int, str]
    :param key: Key of code to construct code/label dictionary for
    :type key: str
    :return: Code/label dictionary for value of passed key
    :rtype: Dict[str, Any]
    """
    entry = {}
    if key in lookup_table_raw_xml:
        code = get_code(lookup_table_raw_xml, key)
        entry["code"] = code
        entry["label"] = lookup_table_local.get(code)
    return entry


def get_institution(raw_inst_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes raw institution data as a dictionary and returns a constructed dictionary with ukprn data.

    :param raw_inst_data: Institution data as a dictionary
    :type raw_inst_data: Dict[str, Any]
    :return: Constructed dictionary with ukprn data.
    :rtype: Dict[str, Any]
    """
    return {
        "pub_ukprn_name": "n/a",
        "pub_ukprn_welsh_name": "n/a",
        "pub_ukprn": raw_inst_data["PUBUKPRN"],
        "ukprn_name": "n/a",
        "ukprn_welsh_name": "n/a",
        "ukprn": raw_inst_data["UKPRN"],
    }


def get_links(raw_inst_data: Dict[str, Any], raw_course_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes raw course and institution data, and returns a dictionary containing the corresponding URLs for each page.

    :param raw_inst_data: Data dictionary for institution
    :type raw_inst_data: Dict[str, Any]
    :param raw_course_data: Data dictionary for course
    :type raw_inst_data: Dict[str, Any]
    :return: Dictionary containing URLs for the provided course and institution
    :rtype: Dict[str, Any]
    """
    links = {}

    item_details = [
        ("ASSURL", "assessment_method", raw_course_data),
        ("CRSECSTURL", "course_cost", raw_course_data),
        ("CRSEURL", "course_page", raw_course_data),
        ("EMPLOYURL", "employment_details", raw_course_data),
        ("SUPPORTURL", "financial_support_details", raw_course_data),
        ("LTURL", "learning_and_teaching_methods", raw_course_data),
        ("SUURL", "student_union", raw_inst_data),
    ]

    for item_detail in item_details:
        link_item = get_english_welsh_item(item_detail[0], item_detail[2])
        if link_item:
            links[item_detail[1]] = link_item

    return links


def get_location_items(
        locations: Locations,
        locids: List[str],
        raw_course_data: Dict[str, Any],
        pub_ukprn: str
) -> List[Dict[str, Any]]:
    """
    Returns a list of location data based on the passed raw course data.
    If the raw course data does not have a location, returns an empty list.

    :param locations: Locations object with a lookup dictionary
    :type locations: Locations
    :param locids: List of location IDs
    :type locids: List[str]
    :param raw_course_data: Raw course data to extract location data from
    :type raw_course_data: Dict[str, Any]
    :param pub_ukprn: pub_ukprn added to location lookup ID
    :type pub_ukprn: str
    :return: List of location data from raw course data
    :rtype: List[Dict[str, Any]]
    """
    location_items = []
    if "COURSELOCATION" not in raw_course_data:
        return location_items

    course_locations = SharedUtils.get_raw_list(
        raw_course_data, "COURSELOCATION"
    ) if SharedUtils.get_raw_list(
        raw_course_data, "COURSELOCATION"
    ) else []
    item = {}
    for course_location in course_locations:
        if "LOCID" not in course_location:
            continue

        if "UCASCOURSEID" in course_location:
            lookup_key = course_location.get("LOCID") + pub_ukprn
            item[lookup_key] = course_location["UCASCOURSEID"]

    for locid in locids:
        location_dict = {}
        raw_location_data = locations.get_location_data_for_key(locid)

        if raw_location_data is None:
            logging.warning(f"failed to find location data in lookup table")

        links, accommodation, student_union = {}, {}, {}
        accommodation = get_english_welsh_item("ACCOMURL", raw_location_data)
        if accommodation:
            links["accommodation"] = accommodation

        student_union = get_english_welsh_item("SUURL", raw_location_data)
        if student_union:
            links["student_union"] = student_union

        if links:
            location_dict["links"] = links

        if "LATITUDE" in raw_location_data:
            location_dict["latitude"] = raw_location_data["LATITUDE"]
        if "LONGITUDE" in raw_location_data:
            location_dict["longitude"] = raw_location_data["LONGITUDE"]

        name = get_english_welsh_item("LOCNAME", raw_location_data)
        if name:
            location_dict["name"] = name

        if locid in item:
            location_dict["ucas_course_id"] = item[locid]

        location_items.append(location_dict)
    return location_items


def process_stats(
        primary_dataset: List[Dict[str, Any]],
        secondary_dataset: List[Dict[str, Any]],
        tertiary_dataset: List[Dict[str, Any]],
        course_mode: int,
        course_level: int,
        salary_lookup: SectorSalaries,
) -> List[Dict[str, Any]]:
    """
    Processes datasets for construction of sector salary JSONs.

    :param primary_dataset: Primary dataset for JSON construction
    :type primary_dataset: List[Dict[str, Any]]
    :param secondary_dataset: Secondary dataset for JSON construction
    :type secondary_dataset: List[Dict[str, Any]]
    :param tertiary_dataset: Tertiary dataset for json construction
    :type tertiary_dataset: List[Dict[str, Any]]
    :param course_mode: Course mode
    :type course_mode: int
    :param course_level: Course level
    :type course_level: int
    :param salary_lookup: SectorSalaries object containing lookup dictionaries
    :type salary_lookup: SectorSalaries
    :return: List of dictionaries containing JSONs with sector salary data
    :rtype: List[Dict[str, Any]]
    """
    xml_array = []
    for index, primary_inst in enumerate(primary_dataset):
        # If the subject is unavailable for the specific source (GO/LEO3/LEO5), use a sibling source instead.
        key_subject_code = None
        if 'subject' in primary_inst and 'code' in primary_inst['subject']:
            key_subject_code = primary_inst['subject']['code']
        elif len(secondary_dataset) >= (index + 1) and 'subject' in secondary_dataset[index] and 'code' in \
                secondary_dataset[index]['subject']:
            key_subject_code = secondary_dataset[index]['subject']['code']
        elif len(tertiary_dataset) >= (index + 1) and 'subject' in tertiary_dataset[index] and 'code' in \
                tertiary_dataset[index]['subject']:
            key_subject_code = tertiary_dataset[index]['subject']['code']

        if key_subject_code is not None:
            lookup_key = (
                f"{key_subject_code}-{course_mode}-{course_level}"
            )
            go_sector_salary = salary_lookup.get_sector_salaries_data_for_key(
                lookup_key
            )

            if go_sector_salary is not None:
                xml_array.append(go_sector_salary)
    return xml_array


def get_go_sector_json(
        go_salary_inst_list: List[Dict[str, Any]],
        leo3_salary_inst_list: List[Dict[str, Any]],
        leo5_salary_inst_list: List[Dict[str, Any]],
        go_sector_salary_lookup: SectorSalaries,
        course_mode: int,
        course_level: int,
        subject_enricher: SubjectCourseEnricher
) -> List[Dict[str, Any]]:
    """
    Builds a JSON list for the GO sector mapping.

    :param go_salary_inst_list: List of institution data for GO salaries
    :type go_salary_inst_list: List[Dict[str, Any]]
    :param leo3_salary_inst_list: List of institution data for LEO3 salaries
    :type leo3_salary_inst_list: List[Dict[str, Any]]
    :param leo5_salary_inst_list: List of institution data for LEO5 salaries
    :type leo5_salary_inst_list: List[Dict[str, Any]]
    :param go_sector_salary_lookup: SectorSalaries object containing lookup dictionaries
    :type go_sector_salary_lookup: SectorSalaries
    :param course_mode: Course mode
    :type course_mode: int
    :param course_level: Course level
    :type course_level: int
    :param subject_enricher: SubjectCourseEnricher object to add UKRLP data
    :type subject_enricher: SubjectCourseEnricher
    :return: List of JSON dictionaries for GO salary data
    :rtype: List[Dict[str, Any]]
    """
    go_salary_sector_xml_array = process_stats(
        primary_dataset=go_salary_inst_list,
        secondary_dataset=leo3_salary_inst_list,
        tertiary_dataset=leo5_salary_inst_list,
        course_mode=course_mode,
        course_level=course_level,
        salary_lookup=go_sector_salary_lookup
    )

    mapper = GoSalaryMappings("GO", subject_enricher)
    return mapper.map_xml_to_json_array(
        xml_as_array=go_salary_sector_xml_array,
    )


def get_leo3_sector_json(
        leo3_salary_inst_list: List[Dict[str, Any]],
        go_salary_inst_list: List[Dict[str, Any]],
        leo5_salary_inst_list: List[Dict[str, Any]],
        leo3_sector_salary_lookup: SectorSalaries,
        course_mode: int,
        course_level: int,
        subject_enricher: SubjectCourseEnricher
) -> List[Dict[str, Any]]:
    """
    Builds a JSON list for the LEO3 sector mapping.

    :param leo3_salary_inst_list: List of institution data for LEO3 salaries
    :type leo3_salary_inst_list: List[Dict[str, Any]]
    :param go_salary_inst_list: List of institution data for GO salaries
    :type go_salary_inst_list: List[Dict[str, Any]]
    :param leo5_salary_inst_list: List of institution data for LEO5 salaries
    :type leo5_salary_inst_list: List[Dict[str, Any]]
    :param leo3_sector_salary_lookup: SectorSalaries object containing lookup dictionaries
    :type leo3_sector_salary_lookup: SectorSalaries
    :param course_mode: Course mode
    :type course_mode: int
    :param course_level: Course level
    :type course_level: int
    :param subject_enricher: SubjectCourseEnricher object to add UKRLP data
    :type subject_enricher: SubjectCourseEnricher
    :return: List of JSON dictionaries for LEO3 salary data
    :rtype: List[Dict[str, Any]]
    """
    leo3_sector_xml_array = process_stats(
        primary_dataset=leo3_salary_inst_list,
        secondary_dataset=go_salary_inst_list,
        tertiary_dataset=leo5_salary_inst_list,
        course_mode=course_mode,
        course_level=course_level,
        salary_lookup=leo3_sector_salary_lookup
    )

    mapper = LeoSectorMappings("LEO3", subject_enricher)
    return mapper.map_xml_to_json_array(
        xml_as_array=leo3_sector_xml_array,
    )


def get_leo5_sector_json(
        leo5_salary_inst_list: List[Dict[str, Any]],
        go_salary_inst_list: List[Dict[str, Any]],
        leo3_salary_inst_list: List[Dict[str, Any]],
        leo5_sector_salary_lookup: SectorSalaries,
        course_mode: int,
        course_level: int,
        subject_enricher: SubjectCourseEnricher
) -> List[Dict[str, Any]]:
    """
    Builds a JSON list for the LEO5 sector mapping.

    :param leo5_salary_inst_list: List of institution data for LEO5 salaries
    :type leo5_salary_inst_list: List[Dict[str, Any]]
    :param go_salary_inst_list: List of institution data for GO salaries
    :type go_salary_inst_list: List[Dict[str, Any]]
    :param leo3_salary_inst_list: List of institution data for LEO3 salaries
    :type leo3_salary_inst_list: List[Dict[str, Any]]
    :param leo5_sector_salary_lookup: SectorSalaries object containing lookup dictionaries
    :type leo5_sector_salary_lookup: SectorSalaries
    :param course_mode: Course mode
    :type course_mode: int
    :param course_level: Course level
    :type course_level: int
    :param subject_enricher: SubjectCourseEnricher object to add UKRLP data
    :type subject_enricher: SubjectCourseEnricher
    :return: List of JSON dictionaries for LEO5 salary data
    :rtype: List[Dict[str, Any]]
    """
    leo5_sector_xml_array = process_stats(
        primary_dataset=leo5_salary_inst_list,
        secondary_dataset=go_salary_inst_list,
        tertiary_dataset=leo3_salary_inst_list,
        course_mode=course_mode,
        course_level=course_level,
        salary_lookup=leo5_sector_salary_lookup
    )

    mapper = LeoSectorMappings("LEO5", subject_enricher)
    return mapper.map_xml_to_json_array(
        xml_as_array=leo5_sector_xml_array,
    )


def get_code(lookup_table_raw_xml: Dict[str, Any], key: str) -> Union[int, str]:
    """
    Takes a lookup table dictionary and a key, and returns the corresponding code as an integer if it's a digit,
    otherwise returns the code as a string.

    :param lookup_table_raw_xml: Lookup table dictionary
    :type lookup_table_raw_xml: Dict[str, Any]
    :param key: Key to extract code
    :type key: str
    :return: Code as an integer if it's a digit, otherwise a string
    :rtype: Union[int, str]
    """
    code = lookup_table_raw_xml[key]
    if code.isdigit():
        code = int(code)
    return code


def get_qualification(lookup_table_raw_xml: Dict[str, Any], kisaims: KisAims) -> Dict[str, Any]:
    """
    Takes a lookup table XML and a KisAims object, and if the lookup table contains a KIS aim code element, returns
    a dictionary with the corresponding code and label. Otherwise returns an empty dictionary.

    :param lookup_table_raw_xml: Lookup table dictionary
    :type lookup_table_raw_xml: Dict[str, Any]
    :param kisaims: KisAims object containing lookup dictionary
    :return: Constructed dictionary containing KIS code data, or an empty dictionary
    :rtype: Dict[str, Any]
    """
    entry = {}
    if "KISAIMCODE" in lookup_table_raw_xml:
        code = lookup_table_raw_xml["KISAIMCODE"]
        label = kisaims.get_kisaim_label_for_key(code)
        entry["code"] = code
        if label:
            entry["label"] = label
    return entry


def get_kis_aim_label(code: str, kisaims: KisAims) -> str:
    """
    Takes a code and KisAims object and returns the corresponding kis aim label.

    :param code: KIS code
    :param kisaims: KisAims object containing KIS aims lookup dictionary
    :return: Corresponding KIS aims label
    :rtype: str
    """
    label = kisaims.get_kisaim_label_for_key(code)
    return label
