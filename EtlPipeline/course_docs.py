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
import json
import logging
import os
import sys
import time
import traceback
from typing import Callable
from typing import List

import defusedxml.ElementTree as ET
import xmltodict

from EtlPipeline.mappings.go.institution import GoInstitutionMappings
from EtlPipeline.mappings.go.salary import GoSalaryMappings
from EtlPipeline.mappings.go.voice import GoVoiceMappings
from EtlPipeline.mappings.leo.institution import LeoInstitutionMappings
from EtlPipeline.mappings.leo.sector import LeoSectorMappings

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

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
        logging.info(f"Ingesting course for: ({raw_inst_data['PUBUKPRN']})")
        for course in institution.findall("KISCOURSE"):
            raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
            logging.info(
                f"Ingesting course for: {raw_inst_data['PUBUKPRN']}/{raw_course_data['KISCOURSEID']}/{raw_course_data['KISMODE']}) | start")
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
                    leo5_sector_salaries,
                    g_subject_enricher
                )
                enricher.enrich_course(course_doc)
                subject_enricher.enrich_course(course_doc)
                qualification_enricher.enrich_course(course_doc)
                logging.info(f"*****,{json.dumps(course_doc)},{raw_inst_data['PUBUKPRN']}/{raw_course_data['KISCOURSEID']}),doc created")
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
                logging.warning(f"FAILED: Ingesting course for: {raw_inst_data['PUBUKPRN']}/{raw_course_data['KISCOURSEID']}/{raw_course_data['KISMODE']}) | end")
                institution_id = raw_inst_data["UKPRN"]
                course_id = raw_course_data["KISCOURSEID"]
                course_mode = raw_course_data["KISMODE"]
                tb = traceback.format_exc()
                exception_text = f"There was an error: {e} when creating the course document for course with institution_id: {institution_id} course_id: {course_id} course_mode: {course_mode} TRACEBACK: {tb}"
                logging.info(exception_text)

                # with open("course_docs_exceptions_{}.txt".format(version), "a") as myfile:
                #     myfile.write(exception_text + "\n")
                #     myfile.write(tb + "\n")
                #     myfile.write(
                #         "================================================================================================\n")

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
        leo5_sector_salaries,
        g_subject_enricher
):
    outer_wrapper = {}
    outer_wrapper["_id"] = utils.get_uuid()
    outer_wrapper["created_at"] = datetime.datetime.utcnow().isoformat()
    outer_wrapper["updated_at"] = datetime.datetime.utcnow().isoformat()
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
        course["go_salary_inst"] = get_go_inst_json(go_inst_xml_nodes, subject_enricher=g_subject_enricher)  # Returns an array.

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

    course["statistics"] = get_stats(
        raw_course_data, course["country"]["code"]
    )

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


def get_go_inst_json(raw_go_inst_data, subject_enricher):
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


def get_leo3_inst_json(raw_leo3_inst_data, subject_enricher) -> List:
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


def get_leo5_inst_json(raw_leo5_inst_data,subject_enricher):
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


def get_go_voice_work_json(raw_go_voice_work_data, subject_enricher):
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


def get_code_label_entry(lookup_table_raw_xml, lookup_table_local, key):
    entry = {}
    if key in lookup_table_raw_xml:
        code = get_code(lookup_table_raw_xml, key)
        entry["code"] = code
        entry["label"] = lookup_table_local.get(code)
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


def process_stats(
        primary_dataset,
        secondary_dataset,
        tertiary_dataset,
        course_mode,
        course_level,
        salary_lookup: Callable,
):
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
        go_salary_inst_list,
        leo3_salary_inst_list,
        leo5_salary_inst_list,
        go_sector_salary_lookup,
        course_mode,
        course_level,
        subject_enricher
):
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
        leo3_salary_inst_list,
        go_salary_inst_list,
        leo5_salary_inst_list,
        leo3_sector_salary_lookup,
        course_mode,
        course_level,
        subject_enricher
):
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
        leo5_salary_inst_list,
        go_salary_inst_list,
        leo3_salary_inst_list,
        leo5_sector_salary_lookup,
        course_mode,
        course_level,
        subject_enricher
):
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
