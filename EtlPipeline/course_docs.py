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
import time
import defusedxml.ElementTree as ET
import traceback

import xmltodict

# TODO investigate setting PATH in Azure so can remove this
CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

g_subject_enricher = None


import course_lookup_tables as lookup
from course_stats import get_stats, SharedUtils, get_earnings_unavail_text
from accreditations import Accreditations
from kisaims import KisAims
from locations import Locations
from ukrlp_enricher import UkRlpCourseEnricher
from subject_enricher import SubjectCourseEnricher
from qualification_enricher import QualificationCourseEnricher
from course_subjects import get_subjects

from sector_salaries import GOSectorSalaries, LEO3SectorSalaries, LEO5SectorSalaries

from SharedCode import utils
from SharedCode.utils import get_english_welsh_item


def load_course_docs(xml_string, version):
    global g_subject_enricher

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
    g_subject_enricher = subject_enricher

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

    go_sector_salaries = GOSectorSalaries(root)
    leo3_sector_salaries = LEO3SectorSalaries(root)
    leo5_sector_salaries = LEO5SectorSalaries(root)

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
                    leo5_sector_salaries
                )
                enricher.enrich_course(course_doc)
                subject_enricher.enrich_course(course_doc)
                qualification_enricher.enrich_course(course_doc)

                new_docs.append(course_doc)
                sproc_count += 1
                course_count += 1

                if sproc_count >= 40:
                    logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
                    cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
                    logging.info(f"Successfully loaded another {sproc_count} documents")
                    # Reset values
                    new_docs = []
                    sproc_count = 0
                    time.sleep(3)
            except Exception as e:

                institution_id = raw_inst_data["UKPRN"]
                course_id = raw_course_data["KISCOURSEID"]
                course_mode = raw_course_data["KISMODE"]

                exception_text = f"There was an error: {e} when creating the course document for course with institution_id: {institution_id} course_id: {course_id} course_mode: {course_mode}"
                logging.info(exception_text)
                tb = traceback.format_exc()
                print(tb)
                with open("course_docs_exceptions_{}.txt".format(version), "a") as myfile:
                    myfile.write(exception_text + "\n")
                    myfile.write(tb + "\n")
                    myfile.write("================================================================================================\n")

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
    go_sector_salaries,
    leo3_sector_salaries,
    leo5_sector_salaries
):
    outer_wrapper = {}
    outer_wrapper["_id"] = utils.get_uuid()
    outer_wrapper["created_at"] = datetime.datetime.utcnow().isoformat()
    outer_wrapper["version"] = version
    outer_wrapper["institution_id"] = raw_inst_data["PUBUKPRN"]
    outer_wrapper["course_id"] = raw_course_data["KISCOURSEID"]
    outer_wrapper["course_mode"] = int(raw_course_data["KISMODE"])
    outer_wrapper["course_level"] = int(raw_course_data["KISLEVEL"])
    outer_wrapper["partition_key"] = str(version)

    course = {}
    course["course_level"] = int(raw_course_data["KISLEVEL"])

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
        course["go_salary_inst"] = get_go_inst_json(go_inst_xml_nodes) # Returns an array.

    leo3_inst_xml_nodes = raw_course_data["LEO3"]
    if leo3_inst_xml_nodes:
        course["leo3_inst"] = get_leo3_inst_json(leo3_inst_xml_nodes)

    leo5_inst_xml_nodes = raw_course_data["LEO5"]
    if leo5_inst_xml_nodes:
        course["leo5_inst"] = get_leo5_inst_json(leo5_inst_xml_nodes)

    go_voice_work_xml_nodes = raw_course_data["GOVOICEWORK"]
    if go_voice_work_xml_nodes:
        course["go_voice_work"] = get_go_voice_work_json(go_voice_work_xml_nodes)

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
    kis_aim_code = raw_course_data["KISAIMCODE"] # KISAIMCODE is guaranteed to exist and have a non-null value.
    kis_aim_label = get_kis_aim_label(kis_aim_code, kisaims)
    if title and title['english'] and kis_aim_label and title['english'] == kis_aim_label:
        course["title"] = title # TODO: change this statement as appropriate, not sure how yet - awaiting OfS requirement.
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

    course["statistics"] = get_stats(
        raw_course_data, course["country"]["code"]
    )

    # Extract the appropriate sector-level earnings data for the current course.
    go_sector_json_array = get_go_sector_json(
        course["go_salary_inst"], course["leo3_inst"], course["leo5_inst"], go_sector_salaries, outer_wrapper["course_mode"], outer_wrapper["course_level"]
    )
    if go_sector_json_array:
        course["go_salary_sector"] = go_sector_json_array

    leo3_sector_json_array = get_leo3_sector_json(
        course["leo3_inst"], course["go_salary_inst"], course["leo5_inst"], leo3_sector_salaries, outer_wrapper["course_mode"], outer_wrapper["course_level"]
    )
    if leo3_sector_json_array:
        course["leo3_salary_sector"] = leo3_sector_json_array

    leo5_sector_json_array = get_leo5_sector_json(
        course["leo5_inst"], course["go_salary_inst"], course["leo3_inst"], leo5_sector_salaries, outer_wrapper["course_mode"], outer_wrapper["course_level"]
    )
    if leo5_sector_json_array:
        course["leo5_salary_sector"] = leo5_sector_json_array

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


def get_go_inst_json(raw_go_inst_data):
    go_salary_array = []

    # For joint courses, we may get passed an OrderedDict of GOSAL records.
    # For single-subject courses, not sure if we get passed an OrderedDict of 1 or something else.
    if raw_go_inst_data:
        if isinstance(raw_go_inst_data, dict):
            unavail_text_english = ""
            unavail_text_welsh = ""
            go_salary = {}
            if 'GOSALUNAVAILREASON' in raw_go_inst_data:
                go_salary["unavail_reason"] = raw_go_inst_data["GOSALUNAVAILREASON"]
                unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "go",
                                                                                     go_salary["unavail_reason"])
            if 'GOSALPOP' in raw_go_inst_data: go_salary["pop"] = raw_go_inst_data["GOSALPOP"]
            if 'GOSALRESP_RATE' in raw_go_inst_data: go_salary["resp_rate"] = raw_go_inst_data["GOSALRESP_RATE"]
            if 'GOSALAGG' in raw_go_inst_data: go_salary["agg"] = raw_go_inst_data["GOSALAGG"]
            if 'GOSALSBJ' in raw_go_inst_data: go_salary["subject"] = get_subject(raw_go_inst_data["GOSALSBJ"])
            if 'GOINSTLQ' in raw_go_inst_data: go_salary["lq"] = raw_go_inst_data["GOINSTLQ"]
            if 'GOINSTMED' in raw_go_inst_data: go_salary["med"] = raw_go_inst_data["GOINSTMED"]
            if 'GOINSTUQ' in raw_go_inst_data: go_salary["uq"] = raw_go_inst_data["GOINSTUQ"]
            if 'GOPROV_PC_UK' in raw_go_inst_data: go_salary["inst_prov_pc_uk"] = raw_go_inst_data["GOPROV_PC_UK"]
            if 'GOPROV_PC_E' in raw_go_inst_data: go_salary["inst_prov_pc_e"] = raw_go_inst_data["GOPROV_PC_E"]
            if 'GOPROV_PC_NI' in raw_go_inst_data: go_salary["inst_prov_pc_ni"] = raw_go_inst_data["GOPROV_PC_NI"]
            if 'GOPROV_PC_S' in raw_go_inst_data: go_salary["inst_prov_pc_s"] = raw_go_inst_data["GOPROV_PC_S"]
            if 'GOPROV_PC_W' in raw_go_inst_data: go_salary["inst_prov_pc_w"] = raw_go_inst_data["GOPROV_PC_W"]

            go_salary["unavail_text_english"] = unavail_text_english
            go_salary["unavail_text_welsh"] = unavail_text_welsh

            if 'agg' in go_salary and 'subject' in go_salary:
                go_salary["earnings_agg_unavail_message"] = get_earnings_agg_unavail_messages(go_salary["agg"], go_salary["subject"])

            go_salary_array.append(go_salary)
        else:
            for elem in raw_go_inst_data:
                unavail_text_english = ""
                unavail_text_welsh = ""
                go_salary = {}
                if 'GOSALUNAVAILREASON' in elem:
                    go_salary["unavail_reason"] = elem["GOSALUNAVAILREASON"]
                    unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "go",
                                                                                         go_salary["unavail_reason"])
                if 'GOSALPOP' in elem: go_salary["pop"] = elem["GOSALPOP"]
                if 'GOSALRESP_RATE' in elem: go_salary["resp_rate"] = elem["GOSALRESP_RATE"]
                if 'GOSALAGG' in elem: go_salary["agg"] = elem["GOSALAGG"]
                if 'GOSALSBJ' in elem: go_salary["subject"] = get_subject(elem["GOSALSBJ"])
                if 'GOINSTLQ' in elem: go_salary["lq"] = elem["GOINSTLQ"]
                if 'GOINSTMED' in elem: go_salary["med"] = elem["GOINSTMED"]
                if 'GOINSTUQ' in elem: go_salary["uq"] = elem["GOINSTUQ"]
                if 'GOPROV_PC_UK' in elem: go_salary["inst_prov_pc_uk"] = elem["GOPROV_PC_UK"]
                if 'GOPROV_PC_E' in elem: go_salary["inst_prov_pc_e"] = elem["GOPROV_PC_E"]
                if 'GOPROV_PC_NI' in elem: go_salary["inst_prov_pc_ni"] = elem["GOPROV_PC_NI"]
                if 'GOPROV_PC_S' in elem: go_salary["inst_prov_pc_s"] = elem["GOPROV_PC_S"]
                if 'GOPROV_PC_W' in elem: go_salary["inst_prov_pc_w"] = elem["GOPROV_PC_W"]

                go_salary["unavail_text_english"] = unavail_text_english
                go_salary["unavail_text_welsh"] = unavail_text_welsh

                if 'agg' in go_salary and 'subject' in go_salary:
                    go_salary["earnings_agg_unavail_message"] = get_earnings_agg_unavail_messages(go_salary["agg"], go_salary["subject"])

                go_salary_array.append(go_salary)
    else:
        # If no GO_SALARY node exists, we still need to display UNAVAIL text.
        unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "go", "1")
        go_salary = {}
        go_salary["unavail_text_english"] = unavail_text_english
        go_salary["unavail_text_welsh"] = unavail_text_welsh
        go_salary_array.append(go_salary)

    return go_salary_array


def get_leo3_inst_json(raw_leo3_inst_data):
    leo3_array = []

    # For joint courses, we may get passed an OrderedDict of LEO3 records.
    # For single-subject courses, not sure if we get passed an OrderedDict of 1 or something else.
    if raw_leo3_inst_data:
        if isinstance(raw_leo3_inst_data, dict):
            unavail_text_english = ""
            unavail_text_welsh = ""
            leo3 = {}
            if 'LEO3UNAVAILREASON' in raw_leo3_inst_data:
                leo3["unavail_reason"] = raw_leo3_inst_data["LEO3UNAVAILREASON"]
                unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo",
                                                                                     leo3["unavail_reason"])
            if 'LEO3POP' in raw_leo3_inst_data: leo3["pop"] = raw_leo3_inst_data["LEO3POP"]
            if 'LEO3AGG' in raw_leo3_inst_data: leo3["agg"] = raw_leo3_inst_data["LEO3AGG"]
            if 'LEO3SBJ' in raw_leo3_inst_data: leo3["subject"] = get_subject(raw_leo3_inst_data["LEO3SBJ"])
            if 'LEO3INSTLQ' in raw_leo3_inst_data: leo3["lq"] = raw_leo3_inst_data["LEO3INSTLQ"]
            if 'LEO3INSTMED' in raw_leo3_inst_data: leo3["med"] = raw_leo3_inst_data["LEO3INSTMED"]
            if 'LEO3INSTUQ' in raw_leo3_inst_data: leo3["uq"] = raw_leo3_inst_data["LEO3INSTUQ"]
            if 'LEO3PROV_PC_UK' in raw_leo3_inst_data: leo3["inst_prov_pc_uk"] = raw_leo3_inst_data["LEO3PROV_PC_UK"]
            if 'LEO3PROV_PC_E' in raw_leo3_inst_data: leo3["inst_prov_pc_e"] = raw_leo3_inst_data["LEO3PROV_PC_E"]
            if 'LEO3PROV_PC_NI' in raw_leo3_inst_data: leo3["inst_prov_pc_ni"] = raw_leo3_inst_data["LEO3PROV_PC_NI"]
            if 'LEO3PROV_PC_S' in raw_leo3_inst_data: leo3["inst_prov_pc_s"] = raw_leo3_inst_data["LEO3PROV_PC_S"]
            if 'LEO3PROV_PC_W' in raw_leo3_inst_data: leo3["inst_prov_pc_w"] = raw_leo3_inst_data["LEO3PROV_PC_W"]
            if 'LEO3PROV_PC_NW' in raw_leo3_inst_data: leo3["inst_prov_pc_nw"] = raw_leo3_inst_data["LEO3PROV_PC_NW"]
            if 'LEO3PROV_PC_NE' in raw_leo3_inst_data: leo3["inst_prov_pc_ne"] = raw_leo3_inst_data["LEO3PROV_PC_NE"]
            if 'LEO3PROV_PC_EM' in raw_leo3_inst_data: leo3["inst_prov_pc_em"] = raw_leo3_inst_data["LEO3PROV_PC_EM"]
            if 'LEO3PROV_PC_WM' in raw_leo3_inst_data: leo3["inst_prov_pc_wm"] = raw_leo3_inst_data["LEO3PROV_PC_WM"]
            if 'LEO3PROV_PC_EE' in raw_leo3_inst_data: leo3["inst_prov_pc_ee"] = raw_leo3_inst_data["LEO3PROV_PC_EE"]
            if 'LEO3PROV_PC_SE' in raw_leo3_inst_data: leo3["inst_prov_pc_se"] = raw_leo3_inst_data["LEO3PROV_PC_SE"]
            if 'LEO3PROV_PC_SW' in raw_leo3_inst_data: leo3["inst_prov_pc_sw"] = raw_leo3_inst_data["LEO3PROV_PC_SW"]
            if 'LEO3PROV_PC_YH' in raw_leo3_inst_data: leo3["inst_prov_pc_yh"] = raw_leo3_inst_data["LEO3PROV_PC_YH"]
            if 'LEO3PROV_PC_LN' in raw_leo3_inst_data: leo3["inst_prov_pc_lo"] = raw_leo3_inst_data["LEO3PROV_PC_LN"]
            if 'LEO3PROV_PC_ED' in raw_leo3_inst_data: leo3["inst_prov_pc_ed"] = raw_leo3_inst_data["LEO3PROV_PC_ED"]
            if 'LEO3PROV_PC_GL' in raw_leo3_inst_data: leo3["inst_prov_pc_gl"] = raw_leo3_inst_data["LEO3PROV_PC_GL"]
            if 'LEO3PROV_PC_CF' in raw_leo3_inst_data: leo3["inst_prov_pc_cf"] = raw_leo3_inst_data["LEO3PROV_PC_CF"]

            leo3["unavail_text_english"] = unavail_text_english
            leo3["unavail_text_welsh"] = unavail_text_welsh

            if 'agg' in leo3 and 'subject' in leo3:
                leo3["earnings_agg_unavail_message"] = get_earnings_agg_unavail_messages(leo3["agg"], leo3["subject"])

            leo3_array.append(leo3)
        else:
            for elem in raw_leo3_inst_data:
                unavail_text_english = ""
                unavail_text_welsh = ""
                leo3 = {}
                if 'LEO3UNAVAILREASON' in elem:
                    leo3["unavail_reason"] = elem["LEO3UNAVAILREASON"]
                    unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo",
                                                                                         leo3["unavail_reason"])
                if 'LEO3POP' in elem: leo3["pop"] = elem["LEO3POP"]
                if 'LEO3AGG' in elem: leo3["agg"] = elem["LEO3AGG"]
                if 'LEO3SBJ' in elem: leo3["subject"] = get_subject(elem["LEO3SBJ"])
                if 'LEO3INSTLQ' in elem: leo3["lq"] = elem["LEO3INSTLQ"]
                if 'LEO3INSTMED' in elem: leo3["med"] = elem["LEO3INSTMED"]
                if 'LEO3INSTUQ' in elem: leo3["uq"] = elem["LEO3INSTUQ"]
                if 'LEO3PROV_PC_UK' in elem: leo3["inst_prov_pc_uk"] = elem["LEO3PROV_PC_UK"]
                if 'LEO3PROV_PC_E' in elem: leo3["inst_prov_pc_e"] = elem["LEO3PROV_PC_E"]
                if 'LEO3PROV_PC_NI' in elem: leo3["inst_prov_pc_ni"] = elem["LEO3PROV_PC_NI"]
                if 'LEO3PROV_PC_S' in elem: leo3["inst_prov_pc_s"] = elem["LEO3PROV_PC_S"]
                if 'LEO3PROV_PC_W' in elem: leo3["inst_prov_pc_w"] = elem["LEO3PROV_PC_W"]
                if 'LEO3PROV_PC_NW' in elem: leo3["inst_prov_pc_nw"] = elem["LEO3PROV_PC_NW"]
                if 'LEO3PROV_PC_NE' in elem: leo3["inst_prov_pc_ne"] = elem["LEO3PROV_PC_NE"]
                if 'LEO3PROV_PC_EM' in elem: leo3["inst_prov_pc_em"] = elem["LEO3PROV_PC_EM"]
                if 'LEO3PROV_PC_WM' in elem: leo3["inst_prov_pc_wm"] = elem["LEO3PROV_PC_WM"]
                if 'LEO3PROV_PC_EE' in elem: leo3["inst_prov_pc_ee"] = elem["LEO3PROV_PC_EE"]
                if 'LEO3PROV_PC_SE' in elem: leo3["inst_prov_pc_se"] = elem["LEO3PROV_PC_SE"]
                if 'LEO3PROV_PC_SW' in elem: leo3["inst_prov_pc_sw"] = elem["LEO3PROV_PC_SW"]
                if 'LEO3PROV_PC_YH' in elem: leo3["inst_prov_pc_yh"] = elem["LEO3PROV_PC_YH"]
                if 'LEO3PROV_PC_LN' in elem: leo3["inst_prov_pc_lo"] = elem["LEO3PROV_PC_LN"]
                if 'LEO3PROV_PC_ED' in elem: leo3["inst_prov_pc_ed"] = elem["LEO3PROV_PC_ED"]
                if 'LEO3PROV_PC_GL' in elem: leo3["inst_prov_pc_gl"] = elem["LEO3PROV_PC_GL"]
                if 'LEO3PROV_PC_CF' in elem: leo3["inst_prov_pc_cf"] = elem["LEO3PROV_PC_CF"]

                leo3["unavail_text_english"] = unavail_text_english
                leo3["unavail_text_welsh"] = unavail_text_welsh

                if 'agg' in leo3 and 'subject' in leo3:
                    leo3["earnings_agg_unavail_message"] = get_earnings_agg_unavail_messages(leo3["agg"], leo3["subject"])

                leo3_array.append(leo3)
    else:
        # If no LEO3 node exists, we still need to display UNAVAIL text.
        unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", "1")
        leo3 = {}
        leo3["unavail_text_english"] = unavail_text_english
        leo3["unavail_text_welsh"] = unavail_text_welsh
        leo3_array.append(leo3)

    return leo3_array


def get_leo5_inst_json(raw_leo5_inst_data):
    leo5_array = []

    # For joint courses, we may get passed an OrderedDict of LEO5 records.
    # For single-subject courses, not sure if we get passed an OrderedDict of 1 or something else.
    if raw_leo5_inst_data:
        if isinstance(raw_leo5_inst_data, dict):
            unavail_text_english = ""
            unavail_text_welsh = ""
            leo5 = {}
            if 'LEO5UNAVAILREASON' in raw_leo5_inst_data:
                leo5["unavail_reason"] = raw_leo5_inst_data["LEO5UNAVAILREASON"]
                unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo",
                                                                                     leo5["unavail_reason"])
            if 'LEO5POP' in raw_leo5_inst_data: leo5["pop"] = raw_leo5_inst_data["LEO5POP"]
            if 'LEO5AGG' in raw_leo5_inst_data: leo5["agg"] = raw_leo5_inst_data["LEO5AGG"]
            if 'LEO5SBJ' in raw_leo5_inst_data: leo5["subject"] = get_subject(raw_leo5_inst_data["LEO5SBJ"])
            if 'LEO5INSTLQ' in raw_leo5_inst_data: leo5["lq"] = raw_leo5_inst_data["LEO5INSTLQ"]
            if 'LEO5INSTMED' in raw_leo5_inst_data: leo5["med"] = raw_leo5_inst_data["LEO5INSTMED"]
            if 'LEO5INSTUQ' in raw_leo5_inst_data: leo5["uq"] = raw_leo5_inst_data["LEO5INSTUQ"]
            if 'LEO5PROV_PC_UK' in raw_leo5_inst_data: leo5["inst_prov_pc_uk"] = raw_leo5_inst_data["LEO5PROV_PC_UK"]
            if 'LEO5PROV_PC_E' in raw_leo5_inst_data: leo5["inst_prov_pc_e"] = raw_leo5_inst_data["LEO5PROV_PC_E"]
            if 'LEO5PROV_PC_NI' in raw_leo5_inst_data: leo5["inst_prov_pc_ni"] = raw_leo5_inst_data["LEO5PROV_PC_NI"]
            if 'LEO5PROV_PC_S' in raw_leo5_inst_data: leo5["inst_prov_pc_s"] = raw_leo5_inst_data["LEO5PROV_PC_S"]
            if 'LEO5PROV_PC_W' in raw_leo5_inst_data: leo5["inst_prov_pc_w"] = raw_leo5_inst_data["LEO5PROV_PC_W"]
            if 'LEO5PROV_PC_NW' in raw_leo5_inst_data: leo5["inst_prov_pc_nw"] = raw_leo5_inst_data["LEO5PROV_PC_NW"]
            if 'LEO5PROV_PC_NE' in raw_leo5_inst_data: leo5["inst_prov_pc_ne"] = raw_leo5_inst_data["LEO5PROV_PC_NE"]
            if 'LEO5PROV_PC_EM' in raw_leo5_inst_data: leo5["inst_prov_pc_em"] = raw_leo5_inst_data["LEO5PROV_PC_EM"]
            if 'LEO5PROV_PC_WM' in raw_leo5_inst_data: leo5["inst_prov_pc_wm"] = raw_leo5_inst_data["LEO5PROV_PC_WM"]
            if 'LEO5PROV_PC_EE' in raw_leo5_inst_data: leo5["inst_prov_pc_ee"] = raw_leo5_inst_data["LEO5PROV_PC_EE"]
            if 'LEO5PROV_PC_SE' in raw_leo5_inst_data: leo5["inst_prov_pc_se"] = raw_leo5_inst_data["LEO5PROV_PC_SE"]
            if 'LEO5PROV_PC_SW' in raw_leo5_inst_data: leo5["inst_prov_pc_sw"] = raw_leo5_inst_data["LEO5PROV_PC_SW"]
            if 'LEO5PROV_PC_YH' in raw_leo5_inst_data: leo5["inst_prov_pc_yh"] = raw_leo5_inst_data["LEO5PROV_PC_YH"]
            if 'LEO5PROV_PC_LN' in raw_leo5_inst_data: leo5["inst_prov_pc_lo"] = raw_leo5_inst_data["LEO5PROV_PC_LN"]
            if 'LEO5PROV_PC_ED' in raw_leo5_inst_data: leo5["inst_prov_pc_ed"] = raw_leo5_inst_data["LEO5PROV_PC_ED"]
            if 'LEO5PROV_PC_GL' in raw_leo5_inst_data: leo5["inst_prov_pc_gl"] = raw_leo5_inst_data["LEO5PROV_PC_GL"]
            if 'LEO5PROV_PC_CF' in raw_leo5_inst_data: leo5["inst_prov_pc_cf"] = raw_leo5_inst_data["LEO5PROV_PC_CF"]

            leo5["unavail_text_english"] = unavail_text_english
            leo5["unavail_text_welsh"] = unavail_text_welsh
            leo5_array.append(leo5)
        else:
            for elem in raw_leo5_inst_data:
                unavail_text_english = ""
                unavail_text_welsh = ""
                leo5 = {}
                if 'LEO5UNAVAILREASON' in elem:
                    leo5["unavail_reason"] = elem["LEO5UNAVAILREASON"]
                    unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo",
                                                                                         leo5["unavail_reason"])
                if 'LEO5POP' in elem: leo5["pop"] = elem["LEO5POP"]
                if 'LEO5AGG' in elem: leo5["agg"] = elem["LEO5AGG"]
                if 'LEO5SBJ' in elem: leo5["subject"] = get_subject(elem["LEO5SBJ"])
                if 'LEO5INSTLQ' in elem: leo5["lq"] = elem["LEO5INSTLQ"]
                if 'LEO5INSTMED' in elem: leo5["med"] = elem["LEO5INSTMED"]
                if 'LEO5INSTUQ' in elem: leo5["uq"] = elem["LEO5INSTUQ"]
                if 'LEO5PROV_PC_UK' in elem: leo5["inst_prov_pc_uk"] = elem["LEO5PROV_PC_UK"]
                if 'LEO5PROV_PC_E' in elem: leo5["inst_prov_pc_e"] = elem["LEO5PROV_PC_E"]
                if 'LEO5PROV_PC_NI' in elem: leo5["inst_prov_pc_ni"] = elem["LEO5PROV_PC_NI"]
                if 'LEO5PROV_PC_S' in elem: leo5["inst_prov_pc_s"] = elem["LEO5PROV_PC_S"]
                if 'LEO5PROV_PC_W' in elem: leo5["inst_prov_pc_w"] = elem["LEO5PROV_PC_W"]
                if 'LEO5PROV_PC_NW' in elem: leo5["inst_prov_pc_nw"] = elem["LEO5PROV_PC_NW"]
                if 'LEO5PROV_PC_NE' in elem: leo5["inst_prov_pc_ne"] = elem["LEO5PROV_PC_NE"]
                if 'LEO5PROV_PC_EM' in elem: leo5["inst_prov_pc_em"] = elem["LEO5PROV_PC_EM"]
                if 'LEO5PROV_PC_WM' in elem: leo5["inst_prov_pc_wm"] = elem["LEO5PROV_PC_WM"]
                if 'LEO5PROV_PC_EE' in elem: leo5["inst_prov_pc_ee"] = elem["LEO5PROV_PC_EE"]
                if 'LEO5PROV_PC_SE' in elem: leo5["inst_prov_pc_se"] = elem["LEO5PROV_PC_SE"]
                if 'LEO5PROV_PC_SW' in elem: leo5["inst_prov_pc_sw"] = elem["LEO5PROV_PC_SW"]
                if 'LEO5PROV_PC_YH' in elem: leo5["inst_prov_pc_yh"] = elem["LEO5PROV_PC_YH"]
                if 'LEO5PROV_PC_LN' in elem: leo5["inst_prov_pc_lo"] = elem["LEO5PROV_PC_LN"]
                if 'LEO5PROV_PC_ED' in elem: leo5["inst_prov_pc_ed"] = elem["LEO5PROV_PC_ED"]
                if 'LEO5PROV_PC_GL' in elem: leo5["inst_prov_pc_gl"] = elem["LEO5PROV_PC_GL"]
                if 'LEO5PROV_PC_CF' in elem: leo5["inst_prov_pc_cf"] = elem["LEO5PROV_PC_CF"]

                leo5["unavail_text_english"] = unavail_text_english
                leo5["unavail_text_welsh"] = unavail_text_welsh
                leo5_array.append(leo5)
    else:
        # If no LEO5 node exists, we still need to display UNAVAIL text.
        unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", "1")
        leo5 = {}
        leo5["unavail_text_english"] = unavail_text_english
        leo5["unavail_text_welsh"] = unavail_text_welsh
        leo5_array.append(leo5)

    return leo5_array


def get_go_voice_work_json(raw_go_voice_work_data):
    go_voice_work_array = []

    if raw_go_voice_work_data:
        if isinstance(raw_go_voice_work_data, dict):
            go_voice_work = {}
            if 'GOWORKSBJ' in raw_go_voice_work_data: go_voice_work["subject"] = get_subject(raw_go_voice_work_data["GOWORKSBJ"])
            if 'GOWORKPOP' in raw_go_voice_work_data: go_voice_work["go_work_pop"] = raw_go_voice_work_data["GOWORKPOP"]
            if 'GOWORKRESP_RATE' in raw_go_voice_work_data: go_voice_work["go_work_resp_rate"] = raw_go_voice_work_data["GOWORKRESP_RATE"]
            if 'GOWORKAGG' in raw_go_voice_work_data: go_voice_work["go_work_agg"] = raw_go_voice_work_data["GOWORKAGG"]
            if 'GOWORKMEAN' in raw_go_voice_work_data: go_voice_work["go_work_mean"] = raw_go_voice_work_data["GOWORKMEAN"]
            if 'GOWORKSKILLS' in raw_go_voice_work_data: go_voice_work["go_work_skills"] = raw_go_voice_work_data["GOWORKSKILLS"]
            if 'GOWORKONTRACK' in raw_go_voice_work_data: go_voice_work["go_work_on_track"] = raw_go_voice_work_data["GOWORKONTRACK"]
            if 'GOWORKUNAVAILREASON' in raw_go_voice_work_data:
                go_voice_work["unavailable"] = get_go_work_unavail_messages("GO", 'GOWORKAGG', "GOWORKUNAVAILREASON", raw_go_voice_work_data)
            go_voice_work_array.append(go_voice_work)
        else:
            for elem in raw_go_voice_work_data:
                go_voice_work = {}
                if 'GOWORKSBJ' in elem: go_voice_work["subject"] = get_subject(elem["GOWORKSBJ"])
                if 'GOWORKAGG' in elem: go_voice_work["go_work_agg"] = elem["GOWORKAGG"]
                if 'GOWORKSKILLS' in elem: go_voice_work["go_work_skills"] = elem["GOWORKSKILLS"]
                if 'GOWORKMEAN' in elem: go_voice_work["go_work_mean"] = elem["GOWORKMEAN"]
                if 'GOWORKONTRACK' in elem: go_voice_work["go_work_on_track"] = elem["GOWORKONTRACK"]
                if 'GOWORKPOP' in elem: go_voice_work["go_work_pop"] = elem["GOWORKPOP"]
                if 'GOWORKRESP_RATE' in elem: go_voice_work["go_work_resp_rate"] = elem["GOWORKRESP_RATE"]
                if 'GOWORKUNAVAILREASON' in elem:
                    go_voice_work["unavailable"] = get_go_work_unavail_messages("GO", 'GOWORKAGG', "GOWORKUNAVAILREASON", elem)
                go_voice_work_array.append(go_voice_work)

    return go_voice_work_array


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


def get_go_sector_json(go_salary_inst_list, leo3_salary_inst_list, leo5_salary_inst_list, go_sector_salary_lookup, course_mode, course_level):
    go_salary_json_array = []

    go_salary_sector_xml_array = []
    for index, go_salary_inst in enumerate(go_salary_inst_list):
        # If the subject is unavailable for the specific source (GO/LEO3/LEO5), use a sibling source instead.
        key_subject_code = None
        if 'subject' in go_salary_inst and 'code' in go_salary_inst['subject']:
            key_subject_code = go_salary_inst['subject']['code']
        elif len(leo3_salary_inst_list) >= (index + 1) and 'subject' in leo3_salary_inst_list[index] and 'code' in leo3_salary_inst_list[index]['subject']:
            key_subject_code = leo3_salary_inst_list[index]['subject']['code']
        elif len(leo5_salary_inst_list) >= (index + 1) and 'subject' in leo5_salary_inst_list[index] and 'code' in leo5_salary_inst_list[index]['subject']:
            key_subject_code = leo5_salary_inst_list[index]['subject']['code']

        if key_subject_code is not None:
            lookup_key = (
                f"{key_subject_code}-{course_mode}-{course_level}"
            )
            go_sector_salary = go_sector_salary_lookup.get_sector_salaries_data_for_key(
                lookup_key
            )

            if go_sector_salary is not None:
                go_salary_sector_xml_array.append(go_sector_salary)
        #else:
            #go_salary_sector_xml_array.append({})

    if go_salary_sector_xml_array and len(go_salary_sector_xml_array) > 0:
        for elem in go_salary_sector_xml_array:
            go_salary = {}
            if 'GOSECSBJ' in elem: go_salary["subject"] = get_subject(elem["GOSECSBJ"])
            if 'KISMODE' in elem: go_salary["mode"] = elem["KISMODE"]
            if 'KISLEVEL' in elem: go_salary["level"] = elem["KISLEVEL"]

            if 'GOSECLQ_UK' in elem: go_salary["lq_uk"] = elem["GOSECLQ_UK"]
            if 'GOSECMED_UK' in elem: go_salary["med_uk"] = elem["GOSECMED_UK"]
            if 'GOSECUQ_UK' in elem: go_salary["uq_uk"] = elem["GOSECUQ_UK"]
            if 'GOSECPOP_UK' in elem: go_salary["pop_uk"] = elem["GOSECPOP_UK"]
            if 'GOSECRESP_UK' in elem: go_salary["resp_uk"] = elem["GOSECRESP_UK"]

            if 'GOSECLQ_E' in elem: go_salary["lq_e"] = elem["GOSECLQ_E"]
            if 'GOSECMED_E' in elem: go_salary["med_e"] = elem["GOSECMED_E"]
            if 'GOSECUQ_E' in elem: go_salary["uq_e"] = elem["GOSECUQ_E"]
            if 'GOSECPOP_E' in elem: go_salary["pop_e"] = elem["GOSECPOP_E"]
            if 'GOSECRESP_E' in elem: go_salary["resp_e"] = elem["GOSECRESP_E"]

            if 'GOSECLQ_S' in elem: go_salary["lq_s"] = elem["GOSECLQ_S"]
            if 'GOSECMED_S' in elem: go_salary["med_s"] = elem["GOSECMED_S"]
            if 'GOSECUQ_S' in elem: go_salary["uq_s"] = elem["GOSECUQ_S"]
            if 'GOSECPOP_S' in elem: go_salary["pop_s"] = elem["GOSECPOP_S"]
            if 'GOSECRESP_S' in elem: go_salary["resp_s"] = elem["GOSECRESP_S"]

            if 'GOSECLQ_W' in elem: go_salary["lq_w"] = elem["GOSECLQ_W"]
            if 'GOSECMED_W' in elem: go_salary["med_w"] = elem["GOSECMED_W"]
            if 'GOSECUQ_W' in elem: go_salary["uq_w"] = elem["GOSECUQ_W"]
            if 'GOSECPOP_W' in elem: go_salary["pop_w"] = elem["GOSECPOP_W"]
            if 'GOSECRESP_W' in elem: go_salary["resp_w"] = elem["GOSECRESP_W"]

            if 'GOSECLQ_NI' in elem: go_salary["lq_ni"] = elem["GOSECLQ_NI"]
            if 'GOSECMED_NI' in elem: go_salary["med_ni"] = elem["GOSECMED_NI"]
            if 'GOSECUQ_NI' in elem: go_salary["uq_ni"] = elem["GOSECUQ_NI"]
            if 'GOSECPOP_NI' in elem: go_salary["pop_ni"] = elem["GOSECPOP_NI"]
            if 'GOSECRESP_NI' in elem: go_salary["resp_ni"] = elem["GOSECRESP_NI"]

            go_salary["unavail_text_region_not_exists_english"], go_salary["unavail_text_region_not_exists_welsh"] =\
                        get_earnings_unavail_text("sector", "go", "region_not_exists")
            go_salary["unavail_text_region_not_nation_english"], go_salary["unavail_text_region_not_nation_welsh"] =\
                        get_earnings_unavail_text("sector", "go", "region_not_nation")
            go_salary_json_array.append(go_salary)
    return go_salary_json_array


def get_leo3_sector_json(leo3_salary_inst_list, go_salary_inst_list, leo5_salary_inst_list, leo3_sector_salary_lookup, course_mode, course_level):
    leo3_json_array = []

    leo3_sector_xml_array = []
    for index, leo3_salary_inst in enumerate(leo3_salary_inst_list):
        # If the subject is unavailable for the specific source (GO/LEO3/LEO5), use a sibling source instead.
        key_subject_code = None
        if 'subject' in leo3_salary_inst and 'code' in leo3_salary_inst['subject']:
            key_subject_code = leo3_salary_inst['subject']['code']
        elif len(go_salary_inst_list) >= (index + 1) and 'subject' in go_salary_inst_list[index] and 'code' in go_salary_inst_list[index]['subject']:
            key_subject_code = go_salary_inst_list[index]['subject']['code']
        elif len(leo5_salary_inst_list) >= (index + 1) and 'subject' in leo5_salary_inst_list[index] and 'code' in leo5_salary_inst_list[index]['subject']:
            key_subject_code = leo5_salary_inst_list[index]['subject']['code']

        if key_subject_code is not None:
            lookup_key = (
                f"{key_subject_code}-{course_mode}-{course_level}"
            )
            leo3_sector_salary = leo3_sector_salary_lookup.get_sector_salaries_data_for_key(
                lookup_key
            )

            if leo3_sector_salary is not None:
                leo3_sector_xml_array.append(leo3_sector_salary)
        #else:
            #leo3_sector_xml_array.append({})

    if leo3_sector_xml_array and len(leo3_sector_xml_array) > 0:
        for elem in leo3_sector_xml_array:
            leo3 = {}
            if 'LEO3SECSBJ' in elem: leo3["subject"] = get_subject(elem["LEO3SECSBJ"])
            if 'KISMODE' in elem: leo3["mode"] = elem["KISMODE"]
            if 'KISLEVEL' in elem: leo3["level"] = elem["KISLEVEL"]

            if 'LEO3LQ_UK' in elem: leo3["lq_uk"] = elem["LEO3LQ_UK"]
            if 'LEO3MED_UK' in elem: leo3["med_uk"] = elem["LEO3MED_UK"]
            if 'LEO3UQ_UK' in elem: leo3["uq_uk"] = elem["LEO3UQ_UK"]
            if 'LEO3SECPOP_UK' in elem: leo3["pop_uk"] = elem["LEO3SECPOP_UK"]

            if 'LEO3LQ_E' in elem: leo3["lq_e"] = elem["LEO3LQ_E"]
            if 'LEO3MED_E' in elem: leo3["med_e"] = elem["LEO3MED_E"]
            if 'LEO3UQ_E' in elem: leo3["uq_e"] = elem["LEO3UQ_E"]
            if 'LEO3SECPOP_E' in elem: leo3["pop_e"] = elem["LEO3SECPOP_E"]

            if 'LEO3LQ_S' in elem: leo3["lq_s"] = elem["LEO3LQ_S"]
            if 'LEO3MED_S' in elem: leo3["med_s"] = elem["LEO3MED_S"]
            if 'LEO3UQ_S' in elem: leo3["uq_s"] = elem["LEO3UQ_S"]
            if 'LEO3SECPOP_S' in elem: leo3["pop_s"] = elem["LEO3SECPOP_S"]

            if 'LEO3LQ_W' in elem: leo3["lq_w"] = elem["LEO3LQ_W"]
            if 'LEO3MED_W' in elem: leo3["med_w"] = elem["LEO3MED_W"]
            if 'LEO3UQ_W' in elem: leo3["uq_w"] = elem["LEO3UQ_W"]
            if 'LEO3SECPOP_W' in elem: leo3["pop_w"] = elem["LEO3SECPOP_W"]

            if 'LEO3LQ_NW' in elem: leo3["lq_nw"] = elem["LEO3LQ_NW"]
            if 'LEO3MED_NW' in elem: leo3["med_nw"] = elem["LEO3MED_NW"]
            if 'LEO3UQ_NW' in elem: leo3["uq_nw"] = elem["LEO3UQ_NW"]
            if 'LEO3SECPOP_NW' in elem: leo3["pop_nw"] = elem["LEO3SECPOP_NW"]

            if 'LEO3LQ_NE' in elem: leo3["lq_ne"] = elem["LEO3LQ_NE"]
            if 'LEO3MED_NE' in elem: leo3["med_ne"] = elem["LEO3MED_NE"]
            if 'LEO3UQ_NE' in elem: leo3["uq_ne"] = elem["LEO3UQ_NE"]
            if 'LEO3SECPOP_NE' in elem: leo3["pop_ne"] = elem["LEO3SECPOP_NE"]

            if 'LEO3LQ_EM' in elem: leo3["lq_em"] = elem["LEO3LQ_EM"]
            if 'LEO3MED_EM' in elem: leo3["med_em"] = elem["LEO3MED_EM"]
            if 'LEO3UQ_EM' in elem: leo3["uq_em"] = elem["LEO3UQ_EM"]
            if 'LEO3SECPOP_EM' in elem: leo3["pop_em"] = elem["LEO3SECPOP_EM"]

            if 'LEO3LQ_WM' in elem: leo3["lq_wm"] = elem["LEO3LQ_WM"]
            if 'LEO3MED_WM' in elem: leo3["med_wm"] = elem["LEO3MED_WM"]
            if 'LEO3UQ_WM' in elem: leo3["uq_wm"] = elem["LEO3UQ_WM"]
            if 'LEO3SECPOP_WM' in elem: leo3["pop_wm"] = elem["LEO3SECPOP_WM"]

            if 'LEO3LQ_EE' in elem: leo3["lq_ee"] = elem["LEO3LQ_EE"]
            if 'LEO3MED_EE' in elem: leo3["med_ee"] = elem["LEO3MED_EE"]
            if 'LEO3UQ_EE' in elem: leo3["uq_ee"] = elem["LEO3UQ_EE"]
            if 'LEO3SECPOP_EE' in elem: leo3["pop_ee"] = elem["LEO3SECPOP_EE"]

            if 'LEO3LQ_SE' in elem: leo3["lq_se"] = elem["LEO3LQ_SE"]
            if 'LEO3MED_SE' in elem: leo3["med_se"] = elem["LEO3MED_SE"]
            if 'LEO3UQ_SE' in elem: leo3["uq_se"] = elem["LEO3UQ_SE"]
            if 'LEO3SECPOP_SE' in elem: leo3["pop_se"] = elem["LEO3SECPOP_SE"]

            if 'LEO3LQ_SW' in elem: leo3["lq_sw"] = elem["LEO3LQ_SW"]
            if 'LEO3MED_SW' in elem: leo3["med_sw"] = elem["LEO3MED_SW"]
            if 'LEO3UQ_SW' in elem: leo3["uq_sw"] = elem["LEO3UQ_SW"]
            if 'LEO3SECPOP_SW' in elem: leo3["pop_sw"] = elem["LEO3SECPOP_SW"]

            if 'LEO3LQ_YH' in elem: leo3["lq_yh"] = elem["LEO3LQ_YH"]
            if 'LEO3MED_YH' in elem: leo3["med_yh"] = elem["LEO3MED_YH"]
            if 'LEO3UQ_YH' in elem: leo3["uq_yh"] = elem["LEO3UQ_YH"]
            if 'LEO3SECPOP_YH' in elem: leo3["pop_yh"] = elem["LEO3SECPOP_YH"]

            if 'LEO3LQ_LN' in elem: leo3["lq_lo"] = elem["LEO3LQ_LN"]
            if 'LEO3MED_LN' in elem: leo3["med_lo"] = elem["LEO3MED_LN"]
            if 'LEO3UQ_LN' in elem: leo3["uq_lo"] = elem["LEO3UQ_LN"]
            if 'LEO3SECPOP_LN' in elem: leo3["pop_lo"] = elem["LEO3SECPOP_LN"]

            if 'LEO3LQ_ED' in elem: leo3["lq_ed"] = elem["LEO3LQ_ED"]
            if 'LEO3MED_ED' in elem: leo3["med_ed"] = elem["LEO3MED_ED"]
            if 'LEO3UQ_ED' in elem: leo3["uq_ed"] = elem["LEO3UQ_ED"]
            if 'LEO3SECPOP_ED' in elem: leo3["pop_ed"] = elem["LEO3SECPOP_ED"]

            if 'LEO3LQ_GL' in elem: leo3["lq_gl"] = elem["LEO3LQ_GL"]
            if 'LEO3MED_GL' in elem: leo3["med_gl"] = elem["LEO3MED_GL"]
            if 'LEO3UQ_GL' in elem: leo3["uq_gl"] = elem["LEO3UQ_GL"]
            if 'LEO3SECPOP_GL' in elem: leo3["pop_gl"] = elem["LEO3SECPOP_GL"]

            if 'LEO3LQ_CF' in elem: leo3["lq_cf"] = elem["LEO3LQ_CF"]
            if 'LEO3MED_CF' in elem: leo3["med_cf"] = elem["LEO3MED_CF"]
            if 'LEO3UQ_CF' in elem: leo3["uq_cf"] = elem["LEO3UQ_CF"]
            if 'LEO3SECPOP_CF' in elem: leo3["pop_cf"] = elem["LEO3SECPOP_CF"]

            leo3["unavail_text_region_not_exists_english"], leo3["unavail_text_region_not_exists_welsh"] =\
                        get_earnings_unavail_text("sector", "leo", "region_not_exists")
            leo3["unavail_text_region_is_ni_english"], leo3["unavail_text_region_is_ni_welsh"] =\
                        get_earnings_unavail_text("sector", "leo", "region_is_ni")
            leo3_json_array.append(leo3)
    return leo3_json_array


def get_leo5_sector_json(leo5_salary_inst_list, go_salary_inst_list, leo3_salary_inst_list, leo5_sector_salary_lookup, course_mode, course_level):
    leo5_json_array = []

    leo5_sector_xml_array = []
    for index, leo5_salary_inst in enumerate(leo5_salary_inst_list):
        # If the subject is unavailable for the specific source (GO/LEO3/LEO5), use a sibling source instead.
        key_subject_code = None
        if 'subject' in leo5_salary_inst and 'code' in leo5_salary_inst['subject']:
            key_subject_code = leo5_salary_inst['subject']['code']
        elif len(go_salary_inst_list) >= (index + 1) and 'subject' in go_salary_inst_list[index] and 'code' in go_salary_inst_list[index]['subject']:
            key_subject_code = go_salary_inst_list[index]['subject']['code']
        elif len(leo3_salary_inst_list) >= (index + 1) and 'subject' in leo3_salary_inst_list[index] and 'code' in leo3_salary_inst_list[index]['subject']:
            key_subject_code = leo3_salary_inst_list[index]['subject']['code']

        if key_subject_code is not None:
            lookup_key = (
                f"{key_subject_code}-{course_mode}-{course_level}"
            )
            leo5_sector_salary = leo5_sector_salary_lookup.get_sector_salaries_data_for_key(
                lookup_key
            )

            if leo5_sector_salary is not None:
                leo5_sector_xml_array.append(leo5_sector_salary)
        #else:
            #leo5_sector_xml_array.append({})

    if leo5_sector_xml_array and len(leo5_sector_xml_array) > 0:
        for elem in leo5_sector_xml_array:
            leo5 = {}
            if 'LEO5SECSBJ' in elem: leo5["subject"] = get_subject(elem["LEO5SECSBJ"])
            if 'KISMODE' in elem: leo5["mode"] = elem["KISMODE"]
            if 'KISLEVEL' in elem: leo5["level"] = elem["KISLEVEL"]

            if 'LEO5LQ_UK' in elem: leo5["lq_uk"] = elem["LEO5LQ_UK"]
            if 'LEO5MED_UK' in elem: leo5["med_uk"] = elem["LEO5MED_UK"]
            if 'LEO5UQ_UK' in elem: leo5["uq_uk"] = elem["LEO5UQ_UK"]
            if 'LEO5SECPOP_UK' in elem: leo5["pop_uk"] = elem["LEO5SECPOP_UK"]

            if 'LEO5LQ_E' in elem: leo5["lq_e"] = elem["LEO5LQ_E"]
            if 'LEO5MED_E' in elem: leo5["med_e"] = elem["LEO5MED_E"]
            if 'LEO5UQ_E' in elem: leo5["uq_e"] = elem["LEO5UQ_E"]
            if 'LEO5SECPOP_E' in elem: leo5["pop_e"] = elem["LEO5SECPOP_E"]

            if 'LEO5LQ_S' in elem: leo5["lq_s"] = elem["LEO5LQ_S"]
            if 'LEO5MED_S' in elem: leo5["med_s"] = elem["LEO5MED_S"]
            if 'LEO5UQ_S' in elem: leo5["uq_s"] = elem["LEO5UQ_S"]
            if 'LEO5SECPOP_S' in elem: leo5["pop_s"] = elem["LEO5SECPOP_S"]

            if 'LEO5LQ_W' in elem: leo5["lq_w"] = elem["LEO5LQ_W"]
            if 'LEO5MED_W' in elem: leo5["med_w"] = elem["LEO5MED_W"]
            if 'LEO5UQ_W' in elem: leo5["uq_w"] = elem["LEO5UQ_W"]
            if 'LEO5SECPOP_W' in elem: leo5["pop_w"] = elem["LEO5SECPOP_W"]

            if 'LEO5LQ_NW' in elem: leo5["lq_nw"] = elem["LEO5LQ_NW"]
            if 'LEO5MED_NW' in elem: leo5["med_nw"] = elem["LEO5MED_NW"]
            if 'LEO5UQ_NW' in elem: leo5["uq_nw"] = elem["LEO5UQ_NW"]
            if 'LEO5SECPOP_NW' in elem: leo5["pop_nw"] = elem["LEO5SECPOP_NW"]

            if 'LEO5LQ_NE' in elem: leo5["lq_ne"] = elem["LEO5LQ_NE"]
            if 'LEO5MED_NE' in elem: leo5["med_ne"] = elem["LEO5MED_NE"]
            if 'LEO5UQ_NE' in elem: leo5["uq_ne"] = elem["LEO5UQ_NE"]
            if 'LEO5SECPOP_NE' in elem: leo5["pop_ne"] = elem["LEO5SECPOP_NE"]

            if 'LEO5LQ_EM' in elem: leo5["lq_em"] = elem["LEO5LQ_EM"]
            if 'LEO5MED_EM' in elem: leo5["med_em"] = elem["LEO5MED_EM"]
            if 'LEO5UQ_EM' in elem: leo5["uq_em"] = elem["LEO5UQ_EM"]
            if 'LEO5SECPOP_EM' in elem: leo5["pop_em"] = elem["LEO5SECPOP_EM"]

            if 'LEO5LQ_WM' in elem: leo5["lq_wm"] = elem["LEO5LQ_WM"]
            if 'LEO5MED_WM' in elem: leo5["med_wm"] = elem["LEO5MED_WM"]
            if 'LEO5UQ_WM' in elem: leo5["uq_wm"] = elem["LEO5UQ_WM"]
            if 'LEO5SECPOP_WM' in elem: leo5["pop_wm"] = elem["LEO5SECPOP_WM"]

            if 'LEO5LQ_EE' in elem: leo5["lq_ee"] = elem["LEO5LQ_EE"]
            if 'LEO5MED_EE' in elem: leo5["med_ee"] = elem["LEO5MED_EE"]
            if 'LEO5UQ_EE' in elem: leo5["uq_ee"] = elem["LEO5UQ_EE"]
            if 'LEO5SECPOP_EE' in elem: leo5["pop_ee"] = elem["LEO5SECPOP_EE"]

            if 'LEO5LQ_SE' in elem: leo5["lq_se"] = elem["LEO5LQ_SE"]
            if 'LEO5MED_SE' in elem: leo5["med_se"] = elem["LEO5MED_SE"]
            if 'LEO5UQ_SE' in elem: leo5["uq_se"] = elem["LEO5UQ_SE"]
            if 'LEO5SECPOP_SE' in elem: leo5["pop_se"] = elem["LEO5SECPOP_SE"]

            if 'LEO5LQ_SW' in elem: leo5["lq_sw"] = elem["LEO5LQ_SW"]
            if 'LEO5MED_SW' in elem: leo5["med_sw"] = elem["LEO5MED_SW"]
            if 'LEO5UQ_SW' in elem: leo5["uq_sw"] = elem["LEO5UQ_SW"]
            if 'LEO5SECPOP_SW' in elem: leo5["pop_sw"] = elem["LEO5SECPOP_SW"]

            if 'LEO5LQ_YH' in elem: leo5["lq_yh"] = elem["LEO5LQ_YH"]
            if 'LEO5MED_YH' in elem: leo5["med_yh"] = elem["LEO5MED_YH"]
            if 'LEO5UQ_YH' in elem: leo5["uq_yh"] = elem["LEO5UQ_YH"]
            if 'LEO5SECPOP_YH' in elem: leo5["pop_yh"] = elem["LEO5SECPOP_YH"]

            if 'LEO5LQ_LN' in elem: leo5["lq_lo"] = elem["LEO5LQ_LN"]
            if 'LEO5MED_LN' in elem: leo5["med_lo"] = elem["LEO5MED_LN"]
            if 'LEO5UQ_LN' in elem: leo5["uq_lo"] = elem["LEO5UQ_LN"]
            if 'LEO5SECPOP_LN' in elem: leo5["pop_lo"] = elem["LEO5SECPOP_LN"]

            if 'LEO5LQ_ED' in elem: leo5["lq_ed"] = elem["LEO5LQ_ED"]
            if 'LEO5MED_ED' in elem: leo5["med_ed"] = elem["LEO5MED_ED"]
            if 'LEO5UQ_ED' in elem: leo5["uq_ed"] = elem["LEO5UQ_ED"]
            if 'LEO5SECPOP_ED' in elem: leo5["pop_ed"] = elem["LEO5SECPOP_ED"]

            if 'LEO5LQ_GL' in elem: leo5["lq_gl"] = elem["LEO5LQ_GL"]
            if 'LEO5MED_GL' in elem: leo5["med_gl"] = elem["LEO5MED_GL"]
            if 'LEO5UQ_GL' in elem: leo5["uq_gl"] = elem["LEO5UQ_GL"]
            if 'LEO5SECPOP_GL' in elem: leo5["pop_gl"] = elem["LEO5SECPOP_GL"]

            if 'LEO5LQ_CF' in elem: leo5["lq_cf"] = elem["LEO5LQ_CF"]
            if 'LEO5MED_CF' in elem: leo5["med_cf"] = elem["LEO5MED_CF"]
            if 'LEO5UQ_CF' in elem: leo5["uq_cf"] = elem["LEO5UQ_CF"]
            if 'LEO5SECPOP_CF' in elem: leo5["pop_cf"] = elem["LEO5SECPOP_CF"]

            leo5["unavail_text_region_not_exists_english"], leo5["unavail_text_region_not_exists_welsh"] =\
                        get_earnings_unavail_text("sector", "leo", "region_not_exists")
            leo5["unavail_text_region_is_ni_english"], leo5["unavail_text_region_is_ni_welsh"] =\
                        get_earnings_unavail_text("sector", "leo", "region_is_ni")
            leo5_json_array.append(leo5)
    return leo5_json_array


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


def get_kis_aim_label(code, kisaims):
    label = kisaims.get_kisaim_label_for_key(code)
    return label


# TODO: This isn't ideal; we should be using the function of the same name in SharedUtils.
#       However, the hectic schedule dictated by the customer does not provide sufficient time.
def get_subject(subject_code):
    subject = {}
    subject["code"] = subject_code
    subject["english_label"] = g_subject_enricher.subject_lookups[subject_code]["english_name"]
    subject["welsh_label"] = g_subject_enricher.subject_lookups[subject_code]["welsh_name"]
    return subject


def create_geo_sector_salary_list(dataset):
    return create_sector_salary_list(dataset, 'GOSECSAL')


def create_leo3_sector_salary_list(dataset):
    return create_sector_salary_list(dataset, 'LEO3SEC')


def create_leo5_sector_salary_list(dataset):
    return create_sector_salary_list(dataset, 'LEO5SEC')


def create_sector_salary_list(dataset, sector_type):
    return [salary for salary in dataset.findall(sector_type)]


def get_go_work_unavail_messages(xml_element_key, xml_agg_key, xml_unavail_reason_key, raw_data_element):
    shared_utils = SharedUtils(
        xml_element_key,
        "GOWORKSBJ",
        xml_agg_key,
        xml_unavail_reason_key,
    )
    return shared_utils.get_unavailable(raw_data_element)


def get_earnings_agg_unavail_messages(agg_value, subject):
    earnings_agg_unavail_messages = {}

    if agg_value in ['21', '22']:
        message_english = "The data displayed is from students on this and other "\
            "courses in [Subject].\n\nThis includes data from this and related courses at the same university or "\
            "college. There was not enough data to publish more specific information. This does not reflect on "\
            "the quality of the course."
        message_welsh = "Daw'r data a ddangosir gan fyfyrwyr ar y cwrs hwn a chyrsiau "\
            "[Subject] eraill.\n\nMae hwn yn cynnwys data o'r cwrs hwn a chyrsiau cysylltiedig yn yr un brifysgol "\
            "neu goleg. Nid oedd digon o ddata ar gael i gyhoeddi gwybodaeth fwy manwl. Nid yw hyn yn adlewyrchu "\
            "ansawdd y cwrs."

        earnings_agg_unavail_messages['english'] = message_english.replace("[Subject]", subject['english_label'])
        earnings_agg_unavail_messages['welsh'] = message_welsh.replace("[Subject]", subject['welsh_label'])

    return earnings_agg_unavail_messages
