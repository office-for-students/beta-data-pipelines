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


# TODO: apw: Remove the leo and salary mappings once they are no longer needed by the front end.
#   i.e. when we make the Sept 2020 changes to the course comparison page.


import course_lookup_tables as lookup
from course_stats import get_stats, SharedUtils
from accreditations import Accreditations
from kisaims import KisAims
from locations import Locations
from ukrlp_enricher import UkRlpCourseEnricher
from subject_enricher import SubjectCourseEnricher
from qualification_enricher import QualificationCourseEnricher
from course_subjects import get_subjects

from go_salary_sector import GoSalarySector
from leo3_sector import Leo3Sector
from leo5_sector import Leo5Sector

from SharedCode import utils
from SharedCode.utils import get_english_welsh_item


def load_course_docs(xml_string, version):
    """Parse HESA XML passed in and create JSON course docs in Cosmos DB."""

    cosmosdb_client = utils.get_cosmos_client()

    logging.info(
        "adding ukrlp data into memory ahead of building course documents"
    )
    enricher = UkRlpCourseEnricher(version)

    logging.info(
        "adding subject data into memory ahead of building course documents"
    )
    subject_enricher = SubjectCourseEnricher(version)

    logging.info(
        "adding qualification data into memory ahead of building course documents"
    )
    qualification_enricher = QualificationCourseEnricher()

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
            try:
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                locids = get_locids(raw_course_data, ukprn)

                # Extract the appropriate sector-level earnings data for the current course.
                go_salary_sector = GoSalarySector(root, raw_course_data).get_matching_sector()
                leo3_sector = Leo3Sector(root, raw_course_data).get_matching_sector()
                leo5_sector = Leo5Sector(root, raw_course_data).get_matching_sector()

                course_doc = get_course_doc(
                    accreditations,
                    locations,
                    locids,
                    raw_inst_data,
                    raw_course_data,
                    kisaims,
                    version,
                    go_salary_sector,
                    leo3_sector,
                    leo5_sector,
                )
                enricher.enrich_course(course_doc)
                subject_enricher.enrich_course(course_doc)
                qualification_enricher.enrich_course(course_doc)

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
            except Exception as e:
                course_id = raw_course_data["KISCOURSEID"]
                logging.info(f"There was an error when creating the course document for course with id: {course_id}")

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
    go_salary_sector, # A single <GOSALARY> node.
    leo3_sector, # A single <LEO3> node.
    leo5_sector, # A single <LEO5> node.
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

    go_salary_node = raw_course_data["GOSALARY"]
    if go_salary_node:
        course["go_salary_inst"] = get_go_salary(go_salary_node)

    leo3_node = raw_course_data["LEO3"]
    if leo3_node:
        course["leo3_inst"] = get_leo3(leo3_node)

    leo5_node = raw_course_data["LEO5"]
    if leo5_node:
        course["leo5_inst"] = get_leo5(leo5_node)

    go_voice_work_node = raw_course_data["GOVOICEWORK"]
    if go_voice_work_node:
        course["go_voice_work"] = get_go_voice_work(go_voice_work_node)

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

    go_salary_sector_items = get_go_salary_sector_items(
        go_salary_sector
    )
    if go_salary_sector_items:
        course["go_salary_sector"] = go_salary_sector_items

    leo3_sector_items = get_leo3_sector_items(
        leo3_sector
    )
    if leo3_sector_items:
        course["leo3_salary_sector"] = leo3_sector_items

    leo5_sector_items = get_leo5_sector_items(
        leo5_sector
    )
    if leo5_sector_items:
        course["leo5_salary_sector"] = leo5_sector_items

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


def get_go_salary(raw_go_salary_data):
    go_salary = {}
    if raw_go_salary_data:
        go_salary["unavail_reason"] = raw_go_salary_data["SALUNAVAILREASON"]
        go_salary["pop"] = raw_go_salary_data["SALPOP"]
        go_salary["resp_rate"] = raw_go_salary_data["SALRESP_RATE"]
        go_salary["agg"] = raw_go_salary_data["SALAGG"]
        go_salary["sbj"] = raw_go_salary_data["SALSBJ"]
        go_salary["lq"] = raw_go_salary_data["INSTLQ"]
        go_salary["med"] = raw_go_salary_data["INSTMED"]
        go_salary["uq"] = raw_go_salary_data["INSTUQ"]
        go_salary["go_inst_prov_pc_uk"] = raw_go_salary_data["GOPROV_PC_UK"]
        go_salary["go_inst_prov_pc_e"] = raw_go_salary_data["GOPROV_PC_E"]
        go_salary["go_inst_prov_pc_s"] = raw_go_salary_data["GOPROV_PC_S"]
        go_salary["go_inst_prov_pc_w"] = raw_go_salary_data["GOPROV_PC_W"]
        go_salary["go_inst_prov_pc_ni"] = raw_go_salary_data["GOPROV_PC_NI"]
    return go_salary


def get_leo3(raw_leo3_data):
    leo3 = {}
    if raw_leo3_data:
        leo3["unavail_reason"] = raw_leo3_data["LEO3UNAVAILREASON"]
        leo3["pop"] = raw_leo3_data["LEO3POP"]
        leo3["resp_rate"] = raw_leo3_data["LEO3RESP_RATE"]
        leo3["agg"] = raw_leo3_data["LEO3AGG"]
        leo3["sbj"] = raw_leo3_data["LEO3SBJ"]
        leo3["lq"] = raw_leo3_data["LEO3INSTLQ"]
        leo3["med"] = raw_leo3_data["LEO3INSTMED"]
        leo3["uq"] = raw_leo3_data["LEO3INSTUQ"]
    return leo3


def get_leo5(raw_leo5_data):
    leo5 = {}
    if raw_leo5_data:
        leo5["unavail_reason"] = raw_leo5_data["LEO5UNAVAILREASON"]
        leo5["pop"] = raw_leo5_data["LEO5POP"]
        leo5["resp_rate"] = raw_leo5_data["LEO5RESP_RATE"]
        leo5["agg"] = raw_leo5_data["LEO5AGG"]
        leo5["sbj"] = raw_leo5_data["LEO5SBJ"]
        leo5["lq"] = raw_leo5_data["LEO5INSTLQ"]
        leo5["med"] = raw_leo5_data["LEO5INSTMED"]
        leo5["uq"] = raw_leo5_data["LEO5INSTUQ"]
    return leo5


def get_go_voice_work(raw_go_voice_work_data):
    go_voice_work = {}
    if raw_go_voice_work_data:
        go_voice_work["go_work_skills"] = raw_go_voice_work_data["GOWORKSKILLS"]
        go_voice_work["go_work_mean"] = raw_go_voice_work_data["GOWORKMEAN"]
        go_voice_work["go_work_on_track"] = raw_go_voice_work_data["GOWORKONTRACK"]
        go_voice_work["go_work_pop"] = raw_go_voice_work_data["GOWORKPOP"]
        go_voice_work["go_work_resp_rate"] = raw_go_voice_work_data["GOWORKRESP_RATE"]
    return go_voice_work


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
        "pub_ukprn_welsh_name": "n/a",
        "pub_ukprn": raw_inst_data["PUBUKPRN"],
        "ukprn_name": "n/a",
        "ukprn_welsh_name": "n/a",
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


def get_go_salary_sector_items(go_salary_sector):
    go_salary = {}
    if go_salary_sector:
        go_salary["unavail_reason"] = go_salary_sector["SALUNAVAILREASON"]
        go_salary["pop"] = go_salary_sector["SALPOP"]
        go_salary["resp_rate"] = go_salary_sector["SALRESP_RATE"]
        go_salary["agg"] = go_salary_sector["SALAGG"]
        go_salary["sbj"] = go_salary_sector["SBJ"]
        go_salary["mode"] = go_salary_sector["MODE"]
        go_salary["level"] = go_salary_sector["LEVEL"]

        go_salary["lq_uk"] = go_salary_sector["GOSECLQ_UK"]
        go_salary["med_uk"] = go_salary_sector["GOSECMED_UK"]
        go_salary["uq_uk"] = go_salary_sector["GOSECUQ_UK"]
        go_salary["pop_uk"] = go_salary_sector["GOSECPOP_UK"]
        go_salary["resp_uk"] = go_salary_sector["GOSECRESP_UK"]

        go_salary["lq_e"] = go_salary_sector["GOSECLQ_E"]
        go_salary["med_e"] = go_salary_sector["GOSECMED_E"]
        go_salary["uq_e"] = go_salary_sector["GOSECUQ_E"]
        go_salary["pop_e"] = go_salary_sector["GOSECPOP_E"]
        go_salary["resp_e"] = go_salary_sector["GOSECRESP_E"]

        go_salary["lq_s"] = go_salary_sector["GOSECLQ_S"]
        go_salary["med_s"] = go_salary_sector["GOSECMED_S"]
        go_salary["uq_s"] = go_salary_sector["GOSECUQ_S"]
        go_salary["pop_s"] = go_salary_sector["GOSECPOP_S"]
        go_salary["resp_s"] = go_salary_sector["GOSECRESP_S"]

        go_salary["lq_w"] = go_salary_sector["GOSECLQ_W"]
        go_salary["med_w"] = go_salary_sector["GOSECMED_W"]
        go_salary["uq_w"] = go_salary_sector["GOSECUQ_W"]
        go_salary["pop_w"] = go_salary_sector["GOSECPOP_W"]
        go_salary["resp_w"] = go_salary_sector["GOSECRESP_W"]

        go_salary["lq_ni"] = go_salary_sector["GOSECLQ_NI"]
        go_salary["med_ni"] = go_salary_sector["GOSECMED_NI"]
        go_salary["uq_ni"] = go_salary_sector["GOSECUQ_NI"]
        go_salary["pop_ni"] = go_salary_sector["GOSECPOP_NI"]
        go_salary["resp_ni"] = go_salary_sector["GOSECRESP_NI"]

    return go_salary


def get_leo3_sector_items(leo3_sector):
    leo3 = {}
    if leo3_sector:
        leo3["unavail_reason"] = leo3_sector["UNAVAILREASON"]
        leo3["pop"] = leo3_sector["POP"]
        leo3["resp_rate"] = leo3_sector["RESP_RATE"]
        leo3["agg"] = leo3_sector["SALAGG"]
        leo3["sbj"] = leo3_sector["SBJ"]
        leo3["mode"] = leo3_sector["MODE"]
        leo3["level"] = leo3_sector["LEVEL"]

        leo3["lq_uk"] = leo3_sector["LEO3SECLQ_UK"]
        leo3["med_uk"] = leo3_sector["LEO3SECMED_UK"]
        leo3["uq_uk"] = leo3_sector["LEO3SECUQ_UK"]
        leo3["pop_uk"] = leo3_sector["LEO3SECPOP_UK"]
        leo3["resp_uk"] = leo3_sector["LEO3SECRESP_UK"]

        leo3["lq_e"] = leo3_sector["LEO3SECLQ_E"]
        leo3["med_e"] = leo3_sector["LEO3SECMED_E"]
        leo3["uq_e"] = leo3_sector["LEO3SECUQ_E"]
        leo3["pop_e"] = leo3_sector["LEO3SECPOP_E"]
        leo3["resp_e"] = leo3_sector["LEO3SECRESP_E"]

        leo3["lq_s"] = leo3_sector["LEO3SECLQ_S"]
        leo3["med_s"] = leo3_sector["LEO3SECMED_S"]
        leo3["uq_s"] = leo3_sector["LEO3SECUQ_S"]
        leo3["pop_s"] = leo3_sector["LEO3SECPOP_S"]
        leo3["resp_s"] = leo3_sector["LEO3SECRESP_S"]

        leo3["lq_w"] = leo3_sector["LEO3SECLQ_W"]
        leo3["med_w"] = leo3_sector["LEO3SECMED_W"]
        leo3["uq_w"] = leo3_sector["LEO3SECUQ_W"]
        leo3["pop_w"] = leo3_sector["LEO3SECPOP_W"]
        leo3["resp_w"] = leo3_sector["LEO3SECRESP_W"]

        leo3["lq_ni"] = leo3_sector["LEO3SECLQ_NI"]
        leo3["med_ni"] = leo3_sector["LEO3SECMED_NI"]
        leo3["uq_ni"] = leo3_sector["LEO3SECUQ_NI"]
        leo3["pop_ni"] = leo3_sector["LEO3SECPOP_NI"]
        leo3["resp_ni"] = leo3_sector["LEO3SECRESP_NI"]

    return leo3


def get_leo5_sector_items(leo5_sector):
    leo5 = {}
    if leo5_sector:
        leo5["unavail_reason"] = leo5_sector["UNAVAILREASON"]
        leo5["pop"] = leo5_sector["POP"]
        leo5["resp_rate"] = leo5_sector["RESP_RATE"]
        leo5["agg"] = leo5_sector["SALAGG"]
        leo5["sbj"] = leo5_sector["SBJ"]
        leo5["mode"] = leo5_sector["MODE"]
        leo5["level"] = leo5_sector["LEVEL"]

        leo5["lq_uk"] = leo5_sector["LEO5SECLQ_UK"]
        leo5["med_uk"] = leo5_sector["LEO5SECMED_UK"]
        leo5["uq_uk"] = leo5_sector["LEO5SECUQ_UK"]
        leo5["pop_uk"] = leo5_sector["LEO5SECPOP_UK"]
        leo5["resp_uk"] = leo5_sector["LEO5SECRESP_UK"]

        leo5["lq_e"] = leo5_sector["LEO5SECLQ_E"]
        leo5["med_e"] = leo5_sector["LEO5SECMED_E"]
        leo5["uq_e"] = leo5_sector["LEO5SECUQ_E"]
        leo5["pop_e"] = leo5_sector["LEO5SECPOP_E"]
        leo5["resp_e"] = leo5_sector["LEO5SECRESP_E"]

        leo5["lq_s"] = leo5_sector["LEO5SECLQ_S"]
        leo5["med_s"] = leo5_sector["LEO5SECMED_S"]
        leo5["uq_s"] = leo5_sector["LEO5SECUQ_S"]
        leo5["pop_s"] = leo5_sector["LEO5SECPOP_S"]
        leo5["resp_s"] = leo5_sector["LEO5SECRESP_S"]

        leo5["lq_w"] = leo5_sector["LEO5SECLQ_W"]
        leo5["med_w"] = leo5_sector["LEO5SECMED_W"]
        leo5["uq_w"] = leo5_sector["LEO5SECUQ_W"]
        leo5["pop_w"] = leo5_sector["LEO5SECPOP_W"]
        leo5["resp_w"] = leo5_sector["LEO5SECRESP_W"]

        leo5["lq_ni"] = leo5_sector["LEO5SECLQ_NI"]
        leo5["med_ni"] = leo5_sector["LEO5SECMED_NI"]
        leo5["uq_ni"] = leo5_sector["LEO5SECUQ_NI"]
        leo5["pop_ni"] = leo5_sector["LEO5SECPOP_NI"]
        leo5["resp_ni"] = leo5_sector["LEO5SECRESP_NI"]

    return leo5


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
