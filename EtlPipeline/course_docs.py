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

from salary_sector import SalarySector

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
                    root
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
    root
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

    # Extract the appropriate sector-level earnings data for the current course.
    go_sector_xml_array = SalarySector(root, raw_course_data, course["go_salary_inst"], "GOSECSAL").get_matching_sector_array()
    leo3_sector_xml_array = SalarySector(root, raw_course_data, course["leo3_inst"], "LEO3SEC").get_matching_sector_array()
    leo5_sector_xml_array = SalarySector(root, raw_course_data, course["leo5_inst"], "LEO5SEC").get_matching_sector_array()
    
    go_sector_json_array = get_go_sector_json(
        go_sector_xml_array
    )
    if go_sector_json_array:
        course["go_salary_sector"] = go_sector_json_array

    leo3_sector_json_array = get_leo3_sector_json(
        leo3_sector_xml_array
    )
    if leo3_sector_json_array:
        course["leo3_salary_sector"] = leo3_sector_json_array

    leo5_sector_json_array = get_leo5_sector_json(
        leo5_sector_xml_array
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
            #if type(elem) == str:
            go_salary = {}
            go_salary["unavail_reason"] = raw_go_inst_data["GOSALUNAVAILREASON"]
            go_salary["pop"] = raw_go_inst_data["GOSALPOP"]
            go_salary["resp_rate"] = raw_go_inst_data["GOSALRESP_RATE"]
            go_salary["agg"] = raw_go_inst_data["GOSALAGG"]
            go_salary["subject"] = get_subject(raw_go_inst_data["GOSALSBJ"])
            go_salary["lq"] = raw_go_inst_data["GOINSTLQ"]
            go_salary["med"] = raw_go_inst_data["GOINSTMED"]
            go_salary["uq"] = raw_go_inst_data["GOINSTUQ"]
            go_salary["inst_prov_pc_uk"] = raw_go_inst_data["GOPROV_PC_UK"]
            go_salary["inst_prov_pc_e"] = raw_go_inst_data["GOPROV_PC_E"]
            go_salary["inst_prov_pc_ni"] = raw_go_inst_data["GOPROV_PC_NI"]
            go_salary["inst_prov_pc_s"] = raw_go_inst_data["GOPROV_PC_S"]
            go_salary["inst_prov_pc_w"] = raw_go_inst_data["GOPROV_PC_W"]

            if go_salary["agg"] is None or go_salary["agg"] == "":
                unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "go", go_salary["unavail_reason"])
                
            go_salary["unavail_text_english"] = unavail_text_english
            go_salary["unavail_text_welsh"] = unavail_text_welsh         
            go_salary_array.append(go_salary)
        else:
            for elem in raw_go_inst_data:
                unavail_text_english = ""
                unavail_text_welsh = ""
                #if type(elem) == str:
                go_salary = {}
                go_salary["unavail_reason"] = elem["GOSALUNAVAILREASON"]
                go_salary["pop"] = elem["GOSALPOP"]
                go_salary["resp_rate"] = elem["GOSALRESP_RATE"]
                go_salary["agg"] = elem["GOSALAGG"]
                go_salary["subject"] = get_subject(elem["GOSALSBJ"])
                go_salary["lq"] = elem["GOINSTLQ"]
                go_salary["med"] = elem["GOINSTMED"]
                go_salary["uq"] = elem["GOINSTUQ"]
                go_salary["inst_prov_pc_uk"] = elem["GOPROV_PC_UK"]
                go_salary["inst_prov_pc_e"] = elem["GOPROV_PC_E"]
                go_salary["inst_prov_pc_ni"] = elem["GOPROV_PC_NI"]
                go_salary["inst_prov_pc_s"] = elem["GOPROV_PC_S"]
                go_salary["inst_prov_pc_w"] = elem["GOPROV_PC_W"]

                if go_salary["agg"] is None or go_salary["agg"] == "":
                    unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "go", go_salary["unavail_reason"])
                    
                go_salary["unavail_text_english"] = unavail_text_english
                go_salary["unavail_text_welsh"] = unavail_text_welsh         
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
            leo3["unavail_reason"] = raw_leo3_inst_data["LEO3UNAVAILREASON"]
            leo3["pop"] = raw_leo3_inst_data["LEO3POP"]
            leo3["agg"] = raw_leo3_inst_data["LEO3AGG"]
            leo3["subject"] = get_subject(raw_leo3_inst_data["LEO3SBJ"])
            leo3["lq"] = raw_leo3_inst_data["LEO3INSTLQ"]
            leo3["med"] = raw_leo3_inst_data["LEO3INSTMED"]
            leo3["uq"] = raw_leo3_inst_data["LEO3INSTUQ"]
            leo3["inst_prov_pc_uk"] = raw_leo3_inst_data["LEO3PROV_PC_UK"]
            leo3["inst_prov_pc_e"] = raw_leo3_inst_data["LEO3PROV_PC_E"]
            leo3["inst_prov_pc_ni"] = raw_leo3_inst_data["LEO3PROV_PC_NI"]
            leo3["inst_prov_pc_s"] = raw_leo3_inst_data["LEO3PROV_PC_S"]
            leo3["inst_prov_pc_w"] = raw_leo3_inst_data["LEO3PROV_PC_W"]
            leo3["inst_prov_pc_nw"] = raw_leo3_inst_data["LEO3PROV_PC_NW"]
            leo3["inst_prov_pc_ne"] = raw_leo3_inst_data["LEO3PROV_PC_NE"]
            leo3["inst_prov_pc_em"] = raw_leo3_inst_data["LEO3PROV_PC_EM"]
            leo3["inst_prov_pc_wm"] = raw_leo3_inst_data["LEO3PROV_PC_WM"]
            leo3["inst_prov_pc_ee"] = raw_leo3_inst_data["LEO3PROV_PC_EE"]
            leo3["inst_prov_pc_se"] = raw_leo3_inst_data["LEO3PROV_PC_SE"]
            leo3["inst_prov_pc_sw"] = raw_leo3_inst_data["LEO3PROV_PC_SW"]
            leo3["inst_prov_pc_yh"] = raw_leo3_inst_data["LEO3PROV_PC_YH"]
            leo3["inst_prov_pc_lo"] = raw_leo3_inst_data["LEO3PROV_PC_LN"]
            leo3["inst_prov_pc_ed"] = raw_leo3_inst_data["LEO3PROV_PC_ED"]
            leo3["inst_prov_pc_gl"] = raw_leo3_inst_data["LEO3PROV_PC_GL"]
            leo3["inst_prov_pc_cf"] = raw_leo3_inst_data["LEO3PROV_PC_CF"]

            if leo3["agg"] is None or leo3["agg"] == "":
                unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", leo3["unavail_reason"])
                
            leo3["unavail_text_english"] = unavail_text_english
            leo3["unavail_text_welsh"] = unavail_text_welsh         
            leo3_array.append(leo3)
        else:
            for elem in raw_leo3_inst_data:
                unavail_text_english = ""
                unavail_text_welsh = ""
                leo3 = {}
                leo3["unavail_reason"] = elem["LEO3UNAVAILREASON"]
                leo3["pop"] = elem["LEO3POP"]
                leo3["agg"] = elem["LEO3AGG"]
                leo3["subject"] = get_subject(elem["LEO3SBJ"])
                leo3["lq"] = elem["LEO3INSTLQ"]
                leo3["med"] = elem["LEO3INSTMED"]
                leo3["uq"] = elem["LEO3INSTUQ"]
                leo3["inst_prov_pc_uk"] = elem["LEO3PROV_PC_UK"]
                leo3["inst_prov_pc_e"] = elem["LEO3PROV_PC_E"]
                leo3["inst_prov_pc_ni"] = elem["LEO3PROV_PC_NI"]
                leo3["inst_prov_pc_s"] = elem["LEO3PROV_PC_S"]
                leo3["inst_prov_pc_w"] = elem["LEO3PROV_PC_W"]
                leo3["inst_prov_pc_nw"] = elem["LEO3PROV_PC_NW"]
                leo3["inst_prov_pc_ne"] = elem["LEO3PROV_PC_NE"]
                leo3["inst_prov_pc_em"] = elem["LEO3PROV_PC_EM"]
                leo3["inst_prov_pc_wm"] = elem["LEO3PROV_PC_WM"]
                leo3["inst_prov_pc_ee"] = elem["LEO3PROV_PC_EE"]
                leo3["inst_prov_pc_se"] = elem["LEO3PROV_PC_SE"]
                leo3["inst_prov_pc_sw"] = elem["LEO3PROV_PC_SW"]
                leo3["inst_prov_pc_yh"] = elem["LEO3PROV_PC_YH"]
                leo3["inst_prov_pc_lo"] = elem["LEO3PROV_PC_LN"]
                leo3["inst_prov_pc_ed"] = elem["LEO3PROV_PC_ED"]
                leo3["inst_prov_pc_gl"] = elem["LEO3PROV_PC_GL"]
                leo3["inst_prov_pc_cf"] = elem["LEO3PROV_PC_CF"]

                if leo3["agg"] is None or leo3["agg"] == "":
                    unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", leo3["unavail_reason"])
                    
                leo3["unavail_text_english"] = unavail_text_english
                leo3["unavail_text_welsh"] = unavail_text_welsh         
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
            leo5["unavail_reason"] = raw_leo5_inst_data["LEO5UNAVAILREASON"]
            leo5["pop"] = raw_leo5_inst_data["LEO5POP"]
            leo5["agg"] = raw_leo5_inst_data["LEO5AGG"]
            leo5["subject"] = get_subject(raw_leo5_inst_data["LEO5SBJ"])
            leo5["lq"] = raw_leo5_inst_data["LEO5INSTLQ"]
            leo5["med"] = raw_leo5_inst_data["LEO5INSTMED"]
            leo5["uq"] = raw_leo5_inst_data["LEO5INSTUQ"]
            leo5["inst_prov_pc_uk"] = raw_leo5_inst_data["LEO5PROV_PC_UK"]
            leo5["inst_prov_pc_e"] = raw_leo5_inst_data["LEO5PROV_PC_E"]
            leo5["inst_prov_pc_ni"] = raw_leo5_inst_data["LEO5PROV_PC_NI"]
            leo5["inst_prov_pc_s"] = raw_leo5_inst_data["LEO5PROV_PC_S"]
            leo5["inst_prov_pc_w"] = raw_leo5_inst_data["LEO5PROV_PC_W"]
            leo5["inst_prov_pc_nw"] = raw_leo5_inst_data["LEO5PROV_PC_NW"]
            leo5["inst_prov_pc_ne"] = raw_leo5_inst_data["LEO5PROV_PC_NE"]
            leo5["inst_prov_pc_em"] = raw_leo5_inst_data["LEO5PROV_PC_EM"]
            leo5["inst_prov_pc_wm"] = raw_leo5_inst_data["LEO5PROV_PC_WM"]
            leo5["inst_prov_pc_ee"] = raw_leo5_inst_data["LEO5PROV_PC_EE"]
            leo5["inst_prov_pc_se"] = raw_leo5_inst_data["LEO5PROV_PC_SE"]
            leo5["inst_prov_pc_sw"] = raw_leo5_inst_data["LEO5PROV_PC_SW"]
            leo5["inst_prov_pc_yh"] = raw_leo5_inst_data["LEO5PROV_PC_YH"]
            leo5["inst_prov_pc_lo"] = raw_leo5_inst_data["LEO5PROV_PC_LN"]
            leo5["inst_prov_pc_ed"] = raw_leo5_inst_data["LEO5PROV_PC_ED"]
            leo5["inst_prov_pc_gl"] = raw_leo5_inst_data["LEO5PROV_PC_GL"]
            leo5["inst_prov_pc_cf"] = raw_leo5_inst_data["LEO5PROV_PC_CF"]

            if leo5["agg"] is None or leo5["agg"] == "":
                unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", leo5["unavail_reason"])
                
            leo5["unavail_text_english"] = unavail_text_english
            leo5["unavail_text_welsh"] = unavail_text_welsh         
            leo5_array.append(leo5)
        else:
            for elem in raw_leo5_inst_data:
                unavail_text_english = ""
                unavail_text_welsh = ""
                leo5 = {}
                leo5["unavail_reason"] = elem["LEO5UNAVAILREASON"]
                leo5["pop"] = elem["LEO5POP"]
                leo5["agg"] = elem["LEO5AGG"]
                leo5["subject"] = get_subject(elem["LEO5SBJ"])
                leo5["lq"] = elem["LEO5INSTLQ"]
                leo5["med"] = elem["LEO5INSTMED"]
                leo5["uq"] = elem["LEO5INSTUQ"]
                leo5["inst_prov_pc_uk"] = elem["LEO5PROV_PC_UK"]
                leo5["inst_prov_pc_e"] = elem["LEO5PROV_PC_E"]
                leo5["inst_prov_pc_ni"] = elem["LEO5PROV_PC_NI"]
                leo5["inst_prov_pc_s"] = elem["LEO5PROV_PC_S"]
                leo5["inst_prov_pc_w"] = elem["LEO5PROV_PC_W"]
                leo5["inst_prov_pc_nw"] = elem["LEO5PROV_PC_NW"]
                leo5["inst_prov_pc_ne"] = elem["LEO5PROV_PC_NE"]
                leo5["inst_prov_pc_em"] = elem["LEO5PROV_PC_EM"]
                leo5["inst_prov_pc_wm"] = elem["LEO5PROV_PC_WM"]
                leo5["inst_prov_pc_ee"] = elem["LEO5PROV_PC_EE"]
                leo5["inst_prov_pc_se"] = elem["LEO5PROV_PC_SE"]
                leo5["inst_prov_pc_sw"] = elem["LEO5PROV_PC_SW"]
                leo5["inst_prov_pc_yh"] = elem["LEO5PROV_PC_YH"]
                leo5["inst_prov_pc_lo"] = elem["LEO5PROV_PC_LN"]
                leo5["inst_prov_pc_ed"] = elem["LEO5PROV_PC_ED"]
                leo5["inst_prov_pc_gl"] = elem["LEO5PROV_PC_GL"]
                leo5["inst_prov_pc_cf"] = elem["LEO5PROV_PC_CF"]

                if leo5["agg"] is None or leo5["agg"] == "":
                    unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", leo5["unavail_reason"])
                    
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
 
    # For joint courses, we may get passed an OrderedDict of GOVOICEWORK records.
    # For single-subject courses, not sure if we get passed an OrderedDict of 1 or something else.
    if raw_go_voice_work_data:
        if isinstance(raw_go_voice_work_data, dict):
            unavail_text_english = ""
            unavail_text_welsh = ""
            go_voice_work = {}
            go_voice_work["go_work_unavail_reason"] = raw_go_voice_work_data["GOWORKUNAVAILREASON"]
            go_voice_work["go_work_pop"] = raw_go_voice_work_data["GOWORKPOP"]
            go_voice_work["go_work_resp_rate"] = raw_go_voice_work_data["GOWORKRESP_RATE"]
            go_voice_work["go_work_agg"] = raw_go_voice_work_data["GOWORKAGG"]
            go_voice_work["go_work_mean"] = raw_go_voice_work_data["GOWORKMEAN"]
            go_voice_work["go_work_skills"] = raw_go_voice_work_data["GOWORKSKILLS"]
            go_voice_work["go_work_on_track"] = raw_go_voice_work_data["GOWORKONTRACK"]
            #go_voice_work["go_work_sbj"] = raw_go_voice_work_data["GOWORKSBJ"] # missing from latest HESA schema.

            # For non-salary nodes, I believe unavail messages are handled in Wagtail-CMS in models.py.
            # if leo5["agg"] is None or leo5["agg"] == "":
            #     unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", leo5["unavail_reason"])
                
            # go_voice_work["unavail_text_english"] = unavail_text_english
            # go_voice_work["unavail_text_welsh"] = unavail_text_welsh         
            go_voice_work_array.append(go_voice_work)
        else:
            for elem in raw_go_voice_work_data:
                unavail_text_english = ""
                unavail_text_welsh = ""
                go_voice_work = {}
                go_voice_work["go_work_unavail_reason"] = elem["GOWORKUNAVAILREASON"]
                go_voice_work["go_work_agg"] = elem["GOWORKAGG"]
                go_voice_work["go_work_skills"] = elem["GOWORKSKILLS"]
                go_voice_work["go_work_mean"] = elem["GOWORKMEAN"]
                go_voice_work["go_work_on_track"] = elem["GOWORKONTRACK"]
                go_voice_work["go_work_pop"] = elem["GOWORKPOP"]
                go_voice_work["go_work_resp_rate"] = elem["GOWORKRESP_RATE"]
                #go_voice_work["go_work_sbj"] = raw_go_voice_work_data["GOWORKSBJ"] # missing from latest HESA schema.

                # For non-salary nodes, I believe unavail messages are handled in Wagtail-CMS in models.py.
                # if leo5["agg"] is None or leo5["agg"] == "":
                #     unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", leo5["unavail_reason"])
                    
                # go_voice_work["unavail_text_english"] = unavail_text_english
                # go_voice_work["unavail_text_welsh"] = unavail_text_welsh         
                go_voice_work_array.append(go_voice_work)
    else: 
        # If no LEO5 node exists, we still need to display UNAVAIL text.
        #unavail_text_english, unavail_text_welsh = get_earnings_unavail_text("institution", "leo", "1")
        go_voice_work = {}
        # go_voice_work["unavail_text_english"] = unavail_text_english
        # go_voice_work["unavail_text_welsh"] = unavail_text_welsh
        # go_voice_work_array.append(go_voice_work)

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


def get_go_sector_json(go_salary_sector_xml_array):
    go_salary_json_array = []

    if go_salary_sector_xml_array:
        for elem in go_salary_sector_xml_array:
            go_salary = {}
            go_salary["subject"] = get_subject(elem["GOSECSBJ"])
            go_salary["mode"] = elem["KISMODE"]
            go_salary["level"] = elem["KISLEVEL"]

            go_salary["lq_uk"] = elem["GOSECLQ_UK"]
            go_salary["med_uk"] = elem["GOSECMED_UK"]
            go_salary["uq_uk"] = elem["GOSECUQ_UK"]
            go_salary["pop_uk"] = elem["GOSECPOP_UK"]
            go_salary["resp_uk"] = elem["GOSECRESP_UK"]

            go_salary["lq_e"] = elem["GOSECLQ_E"]
            go_salary["med_e"] = elem["GOSECMED_E"]
            go_salary["uq_e"] = elem["GOSECUQ_E"]
            go_salary["pop_e"] = elem["GOSECPOP_E"]
            go_salary["resp_e"] = elem["GOSECRESP_E"]

            go_salary["lq_s"] = elem["GOSECLQ_S"]
            go_salary["med_s"] = elem["GOSECMED_S"]
            go_salary["uq_s"] = elem["GOSECUQ_S"]
            go_salary["pop_s"] = elem["GOSECPOP_S"]
            go_salary["resp_s"] = elem["GOSECRESP_S"]

            go_salary["lq_w"] = elem["GOSECLQ_W"]
            go_salary["med_w"] = elem["GOSECMED_W"]
            go_salary["uq_w"] = elem["GOSECUQ_W"]
            go_salary["pop_w"] = elem["GOSECPOP_W"]
            go_salary["resp_w"] = elem["GOSECRESP_W"]

            go_salary["lq_ni"] = elem["GOSECLQ_NI"]
            go_salary["med_ni"] = elem["GOSECMED_NI"]
            go_salary["uq_ni"] = elem["GOSECUQ_NI"]
            go_salary["pop_ni"] = elem["GOSECPOP_NI"]
            go_salary["resp_ni"] = elem["GOSECRESP_NI"]

            go_salary["unavail_text_region_not_exists_english"], go_salary["unavail_text_region_not_exists_welsh"] =\
                        get_earnings_unavail_text("sector", "go", "region_not_exists")
            go_salary["unavail_text_region_not_nation_english"], go_salary["unavail_text_region_not_nation_welsh"] =\
                        get_earnings_unavail_text("sector", "go", "region_not_nation")
            go_salary_json_array.append(go_salary)
    return go_salary_json_array


def get_leo3_sector_json(leo3_sector_xml_array):
    leo3_json_array = []

    if leo3_sector_xml_array:
        for elem in leo3_sector_xml_array:
            leo3 = {}
            leo3["subject"] = get_subject(elem["LEO3SECSBJ"])
            leo3["mode"] = elem["KISMODE"]
            leo3["level"] = elem["KISLEVEL"]

            leo3["lq_uk"] = elem["LEO3LQ_UK"]
            leo3["med_uk"] = elem["LEO3MED_UK"]
            leo3["uq_uk"] = elem["LEO3UQ_UK"]
            leo3["pop_uk"] = elem["LEO3SECPOP_UK"]

            leo3["lq_e"] = elem["LEO3LQ_E"]
            leo3["med_e"] = elem["LEO3MED_E"]
            leo3["uq_e"] = elem["LEO3UQ_E"]
            leo3["pop_e"] = elem["LEO3SECPOP_E"]

            leo3["lq_s"] = elem["LEO3LQ_S"]
            leo3["med_s"] = elem["LEO3MED_S"]
            leo3["uq_s"] = elem["LEO3UQ_S"]
            leo3["pop_s"] = elem["LEO3SECPOP_S"]

            leo3["lq_w"] = elem["LEO3LQ_W"]
            leo3["med_w"] = elem["LEO3MED_W"]
            leo3["uq_w"] = elem["LEO3UQ_W"]
            leo3["pop_w"] = elem["LEO3SECPOP_W"]

            leo3["lq_nw"] = elem["LEO3LQ_NW"]
            leo3["med_nw"] = elem["LEO3MED_NW"]
            leo3["uq_nw"] = elem["LEO3UQ_NW"]
            leo3["pop_nw"] = elem["LEO3SECPOP_NW"]

            leo3["lq_ne"] = elem["LEO3LQ_NE"]
            leo3["med_ne"] = elem["LEO3MED_NE"]
            leo3["uq_ne"] = elem["LEO3UQ_NE"]
            leo3["pop_ne"] = elem["LEO3SECPOP_NE"]

            leo3["lq_em"] = elem["LEO3LQ_EM"]
            leo3["med_em"] = elem["LEO3MED_EM"]
            leo3["uq_em"] = elem["LEO3UQ_EM"]
            leo3["pop_em"] = elem["LEO3SECPOP_EM"]

            leo3["lq_wm"] = elem["LEO3LQ_WM"]
            leo3["med_wm"] = elem["LEO3MED_WM"]
            leo3["uq_wm"] = elem["LEO3UQ_WM"]
            leo3["pop_wm"] = elem["LEO3SECPOP_WM"]
        
            leo3["lq_ee"] = elem["LEO3LQ_EE"]
            leo3["med_ee"] = elem["LEO3MED_EE"]
            leo3["uq_ee"] = elem["LEO3UQ_EE"]
            leo3["pop_ee"] = elem["LEO3SECPOP_EE"]

            leo3["lq_se"] = elem["LEO3LQ_SE"]
            leo3["med_se"] = elem["LEO3MED_SE"]
            leo3["uq_se"] = elem["LEO3UQ_SE"]
            leo3["pop_se"] = elem["LEO3SECPOP_SE"]

            leo3["lq_sw"] = elem["LEO3LQ_SW"]
            leo3["med_sw"] = elem["LEO3MED_SW"]
            leo3["uq_sw"] = elem["LEO3UQ_SW"]
            leo3["pop_sw"] = elem["LEO3SECPOP_SW"]

            leo3["lq_yh"] = elem["LEO3LQ_YH"]
            leo3["med_yh"] = elem["LEO3MED_YH"]
            leo3["uq_yh"] = elem["LEO3UQ_YH"]
            leo3["pop_yh"] = elem["LEO3SECPOP_YH"]

            leo3["lq_lo"] = elem["LEO3LQ_LN"]
            leo3["med_lo"] = elem["LEO3MED_LN"]
            leo3["uq_lo"] = elem["LEO3UQ_LN"]
            leo3["pop_lo"] = elem["LEO3SECPOP_LN"]

            leo3["lq_ed"] = elem["LEO3LQ_ED"]
            leo3["med_ed"] = elem["LEO3MED_ED"]
            leo3["uq_ed"] = elem["LEO3UQ_ED"]
            leo3["pop_ed"] = elem["LEO3SECPOP_ED"]

            leo3["lq_gl"] = elem["LEO3LQ_GL"]
            leo3["med_gl"] = elem["LEO3MED_GL"]
            leo3["uq_gl"] = elem["LEO3UQ_GL"]
            leo3["pop_gl"] = elem["LEO3SECPOP_GL"]

            leo3["lq_cf"] = elem["LEO3LQ_CF"]
            leo3["med_cf"] = elem["LEO3MED_CF"]
            leo3["uq_cf"] = elem["LEO3UQ_CF"]
            leo3["pop_cf"] = elem["LEO3SECPOP_CF"]

            leo3["unavail_text_region_not_exists_english"], leo3["unavail_text_region_not_exists_welsh"] =\
                        get_earnings_unavail_text("sector", "leo", "region_not_exists")
            leo3["unavail_text_region_is_ni_english"], leo3["unavail_text_region_is_ni_welsh"] =\
                        get_earnings_unavail_text("sector", "leo", "region_is_ni")
            leo3_json_array.append(leo3)
    return leo3_json_array


def get_leo5_sector_json(leo5_sector_xml_array):
    leo5_json_array = []

    if leo5_sector_xml_array:
        for elem in leo5_sector_xml_array:
            leo5 = {}
            leo5["subject"] = get_subject(elem["LEO5SECSBJ"])
            leo5["mode"] = elem["KISMODE"]
            leo5["level"] = elem["KISLEVEL"]

            leo5["lq_uk"] = elem["LEO5LQ_UK"]
            leo5["med_uk"] = elem["LEO5MED_UK"]
            leo5["uq_uk"] = elem["LEO5UQ_UK"]
            leo5["pop_uk"] = elem["LEO5SECPOP_UK"]

            leo5["lq_e"] = elem["LEO5LQ_E"]
            leo5["med_e"] = elem["LEO5MED_E"]
            leo5["uq_e"] = elem["LEO5UQ_E"]
            leo5["pop_e"] = elem["LEO5SECPOP_E"]

            leo5["lq_s"] = elem["LEO5LQ_S"]
            leo5["med_s"] = elem["LEO5MED_S"]
            leo5["uq_s"] = elem["LEO5UQ_S"]
            leo5["pop_s"] = elem["LEO5SECPOP_S"]

            leo5["lq_w"] = elem["LEO5LQ_W"]
            leo5["med_w"] = elem["LEO5MED_W"]
            leo5["uq_w"] = elem["LEO5UQ_W"]
            leo5["pop_w"] = elem["LEO5SECPOP_W"]

            leo5["lq_nw"] = elem["LEO5LQ_NW"]
            leo5["med_nw"] = elem["LEO5MED_NW"]
            leo5["uq_nw"] = elem["LEO5UQ_NW"]
            leo5["pop_nw"] = elem["LEO5SECPOP_NW"]

            leo5["lq_ne"] = elem["LEO5LQ_NE"]
            leo5["med_ne"] = elem["LEO5MED_NE"]
            leo5["uq_ne"] = elem["LEO5UQ_NE"]
            leo5["pop_ne"] = elem["LEO5SECPOP_NE"]

            leo5["lq_em"] = elem["LEO5LQ_EM"]
            leo5["med_em"] = elem["LEO5MED_EM"]
            leo5["uq_em"] = elem["LEO5UQ_EM"]
            leo5["pop_em"] = elem["LEO5SECPOP_EM"]

            leo5["lq_wm"] = elem["LEO5LQ_WM"]
            leo5["med_wm"] = elem["LEO5MED_WM"]
            leo5["uq_wm"] = elem["LEO5UQ_WM"]
            leo5["pop_wm"] = elem["LEO5SECPOP_WM"]
        
            leo5["lq_ee"] = elem["LEO5LQ_EE"]
            leo5["med_ee"] = elem["LEO5MED_EE"]
            leo5["uq_ee"] = elem["LEO5UQ_EE"]
            leo5["pop_ee"] = elem["LEO5SECPOP_EE"]

            leo5["lq_se"] = elem["LEO5LQ_SE"]
            leo5["med_se"] = elem["LEO5MED_SE"]
            leo5["uq_se"] = elem["LEO5UQ_SE"]
            leo5["pop_se"] = elem["LEO5SECPOP_SE"]

            leo5["lq_sw"] = elem["LEO5LQ_SW"]
            leo5["med_sw"] = elem["LEO5MED_SW"]
            leo5["uq_sw"] = elem["LEO5UQ_SW"]
            leo5["pop_sw"] = elem["LEO5SECPOP_SW"]

            leo5["lq_yh"] = elem["LEO5LQ_YH"]
            leo5["med_yh"] = elem["LEO5MED_YH"]
            leo5["uq_yh"] = elem["LEO5UQ_YH"]
            leo5["pop_yh"] = elem["LEO5SECPOP_YH"]

            leo5["lq_lo"] = elem["LEO5LQ_LN"]
            leo5["med_lo"] = elem["LEO5MED_LN"]
            leo5["uq_lo"] = elem["LEO5UQ_LN"]
            leo5["pop_lo"] = elem["LEO5SECPOP_LN"]

            leo5["lq_ed"] = elem["LEO5LQ_ED"]
            leo5["med_ed"] = elem["LEO5MED_ED"]
            leo5["uq_ed"] = elem["LEO5UQ_ED"]
            leo5["pop_ed"] = elem["LEO5SECPOP_ED"]

            leo5["lq_gl"] = elem["LEO5LQ_GL"]
            leo5["med_gl"] = elem["LEO5MED_GL"]
            leo5["uq_gl"] = elem["LEO5UQ_GL"]
            leo5["pop_gl"] = elem["LEO5SECPOP_GL"]

            leo5["lq_cf"] = elem["LEO5LQ_CF"]
            leo5["med_cf"] = elem["LEO5MED_CF"]
            leo5["uq_cf"] = elem["LEO5UQ_CF"]
            leo5["pop_cf"] = elem["LEO5SECPOP_CF"]

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


# TODO: This isn't ideal; we should be using the function of the same name in SharedUtils.
#       However, the hectic schedule dictated by the customer does not provide sufficient time.
def get_subject(subject_code):
    subject = {}
    subject["code"] = subject_code
    subject["english_label"] = g_subject_enricher.subject_lookups[subject_code]["english_name"]
    subject["welsh_label"] = g_subject_enricher.subject_lookups[subject_code]["welsh_name"]
    return subject