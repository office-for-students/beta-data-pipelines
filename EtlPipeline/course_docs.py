"""
This module extracts course information from the HESA
XML dataset and writes it in JSON format to Cosmos DB.
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
import time
import defusedxml.ElementTree as ET

import xmltodict

# TODO investigate setting PATH in Azure so can remove this
CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)


import course_lookup_tables as lookup
from course_stats import get_stats, SharedUtils
from accreditations import Accreditations
from kisaims import KisAims
from locations import Locations
from ukrlp_enricher import UkRlpCourseEnricher
from subject_enricher import SubjectCourseEnricher
from course_subjects import get_subjects

from SharedCode import utils
from SharedCode.utils import get_english_welsh_item


def load_course_docs(xml_string, version):
    """Parse HESA XML passed in and create JSON course docs in Cosmos DB."""

    cosmosdb_client = utils.get_cosmos_client()

    logging.info(
        "adding ukrlp data into memory ahead of building course documents"
    )
    enricher = UkRlpCourseEnricher()
    logging.info(
        "adding subject data into memory ahead of building course documents"
    )
    subject_enricher = SubjectCourseEnricher()

    collection_link = utils.get_collection_link(
        "AzureCosmosDbDatabaseId", "AzureCosmosDbCoursesCollectionId"
    )

    # Import the XML dataset
    root = ET.fromstring(xml_string)

    options = {"partitionKey": str(version)}
    sproc_link = collection_link + "/sprocs/bulkImport"

    new_docs = []
    sproc_count = 0

    # Import accreditations, common, kisaims and location nodes
    accreditations = Accreditations(root)
    kisaims = KisAims(root)
    locations = Locations(root)

    course_count = 0
    for institution in root.iter("INSTITUTION"):

        raw_inst_data = xmltodict.parse(ET.tostring(institution))[
            "INSTITUTION"
        ]
        ukprn = raw_inst_data["UKPRN"]
        for course in institution.findall("KISCOURSE"):

            raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
            locids = get_locids(raw_course_data, ukprn)
            course_doc = get_course_doc(
                accreditations,
                locations,
                locids,
                raw_inst_data,
                raw_course_data,
                kisaims,
                version,
            )
            enricher.enrich_course(course_doc)
            subject_enricher.enrich_course(course_doc)

            new_docs.append(course_doc)
            sproc_count += 1
            course_count += 1

            if sproc_count >= 50:
                logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
                cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
                logging.info(f"Successfully loaded another {sproc_count} documents")
                # Reset values
                new_docs = []
                sproc_count = 0
                time.sleep(3)


    if sproc_count > 0:
        logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
        cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
        logging.info(f"Successfully loaded another {sproc_count} documents")
        # Reset values
        new_docs = []
        sproc_count = 0

    logging.info(f"Processed {course_count} courses")


def get_locids(raw_course_data, ukprn):
    """Returns a list of lookup keys for use with the locations class"""
    locids = []
    if "COURSELOCATION" not in raw_course_data:
        return locids
    if isinstance(raw_course_data["COURSELOCATION"], list):
        for val in raw_course_data["COURSELOCATION"]:
            # TODO if UCASCOURSEIDs present, then process accordingly
            # For example, check distant learning is set True. May
            # also need to change function name and return type.
            try:
                locids.append(f"{val['LOCID']}{ukprn}")
            except KeyError:
                # TODO: Handle COURSELOCATION without LOCID.
                # See KISCOURSEID BADE for an example of this.
                # Distant learning may provide a UCASCOURSEID
                # under COURSELOCATION
                pass
    else:
        try:
            locids.append(
                f"{raw_course_data['COURSELOCATION']['LOCID']}{ukprn}"
            )
        except KeyError:
            # TODO: Handle COURSELOCATION without LOCID.
            # See KISCOURSEID BADE for an example of this.
            # Distant learning may provide a UCASCOURSEID
            # under COURSELOCATION
            pass
    return locids


def get_course_doc(
    accreditations,
    locations,
    locids,
    raw_inst_data,
    raw_course_data,
    kisaims,
    version,
):
    outer_wrapper = {}
    outer_wrapper["_id"] = utils.get_uuid()
    outer_wrapper["created_at"] = datetime.datetime.utcnow().isoformat()
    outer_wrapper["version"] = version
    outer_wrapper["institution_id"] = raw_inst_data["PUBUKPRN"]
    outer_wrapper["course_id"] = raw_course_data["KISCOURSEID"]
    outer_wrapper["course_mode"] = int(raw_course_data["KISMODE"])
    outer_wrapper["partition_key"] = str(version)

    course = {}

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
    foundataion_year = get_code_label_entry(
        raw_course_data, lookup.foundation_year_availability, "FOUNDATION"
    )
    if foundataion_year:
        course["foundation_year_availability"] = foundataion_year
    if "HONOURS" in raw_course_data:
        course["honours_award_provision"] = int(raw_course_data["HONOURS"])
    course["institution"] = get_institution(raw_inst_data)
    course["kis_course_id"] = raw_course_data["KISCOURSEID"]
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
    if title:
        course["title"] = title
    if "UCASPROGID" in raw_course_data:
        course["ucas_programme_id"] = raw_course_data["UCASPROGID"]
    year_abroad = get_code_label_entry(
        raw_course_data, lookup.year_abroad, "YEARABROAD"
    )
    if year_abroad:
        course["year_abroad"] = get_code_label_entry(
            raw_course_data, lookup.year_abroad, "YEARABROAD"
        )

    course["statistics"] = get_stats(
        raw_course_data, course["country"]["code"]
    )

    outer_wrapper["course"] = course
    return outer_wrapper


def get_accreditations(raw_course_data, acc_lookup):
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


def get_country(raw_inst_data):
    country = {}
    if "COUNTRY" in raw_inst_data:
        code = raw_inst_data["COUNTRY"]
        country["code"] = code
        country["name"] = lookup.country_code[code]
    return country


def get_code_label_entry(lookup_table_raw_xml, lookup_table_local, key):
    entry = {}
    if key in lookup_table_raw_xml:
        code = get_code(lookup_table_raw_xml, key)
        entry["code"] = code
        entry["label"] = lookup_table_local[code]
    return entry


def get_institution(raw_inst_data):
    return {
        "pub_ukprn_name": "n/a",
        "pub_ukprn": raw_inst_data["PUBUKPRN"],
        "ukprn_name": "n/a",
        "ukprn": raw_inst_data["UKPRN"],
    }


def get_links(raw_inst_data, raw_course_data):
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


def get_location_items(locations, locids, raw_course_data, pub_ukprn):
    location_items = []
    if "COURSELOCATION" not in raw_course_data:
        return location_items

    course_locations = SharedUtils.get_raw_list(
        raw_course_data, "COURSELOCATION"
    )
    item = {}
    for course_location in course_locations:
        if "LOCID" not in course_location:
            continue

        if "UCASCOURSEID" in course_location:
            lookup_key = course_location["LOCID"] + pub_ukprn
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


def get_code(lookup_table_raw_xml, key):
    code = lookup_table_raw_xml[key]
    if code.isdigit():
        code = int(code)
    return code


def get_qualification(lookup_table_raw_xml, kisaims):
    entry = {}
    if "KISAIMCODE" in lookup_table_raw_xml:
        code = lookup_table_raw_xml["KISAIMCODE"]
        label = kisaims.get_kisaim_label_for_key(code)
        entry["code"] = code
        if label:
            entry["label"] = label
    return entry