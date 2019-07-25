"""
This module extracts institution information from the HESA
XML dataset and writes it in JSON format to Cosmos DB.

Currently, we handle unexpected exceptions by letting
them bubble up. This should help flush out problems
during development and testing.
"""
import datetime
import inspect
import logging
import os
import sys
import xml.etree.ElementTree as ET
import json

import xmltodict

# TODO set PATH in Azure and remove this
CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from EtlPipelineBlobTrigger.course_docs import get_code_label_entry
from kisaims import KisAims
from locations import Locations
from SharedCode import utils
from ukrlp_enricher import UkRlpCourseEnricher
import course_lookup_tables as lookup

def get_institution(raw_inst_data):
    return {
        "pub_ukprn_name": "n/a",
        "pub_ukprn": raw_inst_data['PUBUKPRN'],
        "ukprn_name": "n/a",
        "ukprn": raw_inst_data['UKPRN']
    }


def get_country(raw_inst_data):
    country = {}
    if 'COUNTRY' in raw_inst_data:
        code = raw_inst_data['COUNTRY']
        country['code'] = code
        country['name'] = lookup.country_code[code]
    return country


def get_locids(raw_course_data, ukprn):
    """Returns a list of lookup keys for use with the locations class"""
    locids = []
    if 'COURSELOCATION' not in raw_course_data:
        return locids
    if type(raw_course_data['COURSELOCATION']) == list:
        for val in raw_course_data['COURSELOCATION']:
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
                f"{raw_course_data['COURSELOCATION']['LOCID']}{ukprn}")
        except KeyError:
            # TODO: Handle COURSELOCATION without LOCID.
            # See KISCOURSEID BADE for an example of this.
            # Distant learning may provide a UCASCOURSEID
            # under COURSELOCATION
            pass
    return locids


def get_accommodation_links(locations, locids):
    accom = []
    for locid in locids:
        accom_dict = {}
        raw_location_data = locations.get_location_data_for_key(locid)
        if 'ACCOMURL' in raw_location_data:
            accom_dict['english'] = raw_location_data['ACCOMURL']
        if 'ACCOMURLW' in raw_location_data:
            accom_dict['welsh'] = raw_location_data['ACCOMURLW']
        if accom_dict:
            accom.append(accom_dict)
    return accom


def get_eng_welsh_item(key, lookup_table):
    item = {}
    keyw = key + 'W'
    if key in lookup_table:
        item['english'] = lookup_table[key]
    if keyw in lookup_table:
        item['welsh'] = lookup_table[keyw]
    return item


def get_links(locations, locids, raw_inst_data, raw_course_data):
    links = {}
    if locids:
        accommodation = get_accommodation_links(locations, locids)
        links['accommodation'] = accommodation

    item_details = [
        ('ASSURL', 'assessment_method', raw_course_data),
        ('CRSEURL', 'course_page', raw_course_data),
        ('EMPLOYURL', 'employment_details', raw_course_data),
        ('SUPPORTURL', 'financial_support_details', raw_course_data),
        ('LTURL', 'learning_and_teaching_methods', raw_course_data),
        ('SUURL', 'student_union', raw_inst_data)
    ]

    for item_detail in item_details:
        link_item = get_eng_welsh_item(item_detail[0], item_detail[2])
        if link_item:
            links[item_detail[1]] = link_item

    return links


def get_location_items(locations, locids, raw_inst_data, raw_course_data):
    location_items = []
    for locid in locids:
        location_dict = {}
        raw_location_data = locations.get_location_data_for_key(locid)
        if 'LATITUDE' in raw_location_data:
            location_dict['latitude'] = raw_location_data['LATITUDE']
        if 'LONGITUDE' in raw_location_data:
            location_dict['longitude'] = raw_location_data['LONGITUDE']
        name = {}
        if 'LOCNAME' in raw_location_data:
            name['english'] = raw_location_data['LOCNAME']
        if 'LOCNAMEW' in raw_location_data:
            name['welsh'] = raw_location_data['LOCNAMEW']
        if name:
            location_dict['name'] = name
        location_items.append(location_dict)
    return location_items


def get_qualification(lookup_table_raw_xml, kisaims):
    entry = {}
    if 'KISAIMCODE' in lookup_table_raw_xml:
        code = lookup_table_raw_xml['KISAIMCODE']
        label = kisaims.get_kisaim_label_for_key(code)
        entry['code'] = code
        if label:
            entry['label'] = label
    return entry


def get_course(raw_course_data):
    course = {}
    distance_learning = get_code_label_entry(raw_course_data,
                                             lookup.distance_learning_lookup,
                                             'DISTANCE')
    if distance_learning:
        course['distance_learning'] = distance_learning
    if 'HONORS' in raw_course_data:
        course['honours_award_provision'] = raw_course_data['HONORS']
    course['kis_course_id'] = raw_course_data['KISCOURSEID']
    mode = get_code_label_entry(raw_course_data, lookup.mode, 'KISMODE')
    if mode:
        course['mode'] = mode
    return course

def get_courses(institution):
    course_entries = []
    course_count = 0
    for course in institution.findall('KISCOURSE'):
        raw_course_data = xmltodict.parse(ET.tostring(course))['KISCOURSE']
        course_entry = get_course(raw_course_data)
        course_count += 1
        course_entries.append(course_entry)
    logging.info(f"Processed {course_count} courses")
    return course_entries

def get_institution_entry(institution):
    raw_inst_data = xmltodict.parse(
        ET.tostring(institution))['INSTITUTION']
    outer_wrapper = {}
    outer_wrapper['id'] = utils.get_uuid()
    outer_wrapper['created_at'] = datetime.datetime.utcnow().isoformat()
    outer_wrapper['version'] = 1
    outer_wrapper['institution_id'] = raw_inst_data['PUBUKPRN']

    inst = {}
    if 'APROutcome' in raw_inst_data:
        inst['apr_outcome'] = raw_inst_data['APROutcome']
    inst['courses'] = get_courses(institution)
    outer_wrapper['institution'] = inst
    return outer_wrapper


def create_institution_docs(xml_string):
    """Parse HESA XML passed in and create JSON institution docs in Cosmos DB."""

    # TODO Investigate writing docs to CosmosDB in bulk to speed things up.
    logging.info(f"create_institution_docs entry \n")
    cosmosdb_client = utils.get_cosmos_client()

    enricher = UkRlpCourseEnricher()

    collection_link = utils.get_collection_link(
        'AzureCosmosDbDatabaseId', 'AzureCosmosDbInstitutionsCollectionId')

    # Import the XML dataset
    root = ET.fromstring(xml_string)

    # Import kisaims and location nodes
    kisaims = KisAims(root)
    locations = Locations(root)

    inst_count = 0
    for institution in root.iter('INSTITUTION'):
        inst_count += 1
        inst_entry = get_institution_entry(institution)
        cosmosdb_client.CreateItem(collection_link, inst_entry)
    logging.info(f"Processed {inst_count} institutions")
