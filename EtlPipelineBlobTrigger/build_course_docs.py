"""
This module extracts course information from the HESA
XML dataset and writes it in JSON format to CosmoDB.
"""
import datetime
import json
import pprint
import uuid
import xmltodict
import xml.etree.ElementTree as ET
import sys
import course_lookup_tables as lookup

from locations import Locations
from kisaims import KisAims


def get_uuid():
    id = uuid.uuid1()
    return(id.hex)


def build_institution(raw_inst_data):
    return {
        "public_ukprn_name": "n/a",
        "public_ukprn": raw_inst_data['PUBUKPRN'],
        "ukprn_name": "n/a",
        "ukprn": raw_inst_data['UKPRN']
    }


def build_country(raw_inst_data):
    entry = {}
    if 'COUNTRY' in raw_inst_data:
        code = raw_inst_data['COUNTRY']
        entry['code'] = code
        entry['name'] = lookup.country_code[code]
    return entry


def get_locids(raw_course_data, ukprn):
    """Returns a list of lookup keys for use in the locations class"""
    locids = []
    if 'COURSELOCATION' not in raw_course_data:
        return locids
    if type(raw_course_data['COURSELOCATION']) == list:
        for val in raw_course_data['COURSELOCATION']:
            # TODO check if UCASCOURSEIDs could be in here
            # if so need skip over as mean distant learning.
            # we think. We should check that distant learning is set on
            # the course level if this is the case.
            try:
                locids.append(f"{val['LOCID']}{ukprn}")
            except KeyError:
                # Handle COURSELOCATION without LOCID - see KISCOURSEID BADE for an example
                # Distant learning may provide a UCASCOURSEID under COURSELOCATION
                pass
    else:
        try:
            locids.append(
                f"{raw_course_data['COURSELOCATION']['LOCID']}{ukprn}")
        except KeyError:
            # Handle COURSELOCATION without LOCID - see KISCOURSEID BADE for an example
            pass
    return locids


def build_accomodation_links(locations, locids):
    """Returns a list of accomodation links"""
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


def build_eng_welsh_item(key, lookup_table):
    item = {}
    keyw = key + 'W'
    if key in lookup_table:
        item['english'] = lookup_table[key]
    if keyw in lookup_table:
        item['welsh'] = lookup_table[keyw]
    return item


def build_links(locations, locids, raw_inst_data, raw_course_data):
    links = {}

    if locids:
        # Handle accomodation separately to put in array array
        accomodation = build_accomodation_links(locations, locids)
        links['accomodation'] = accomodation

    item_details = [('ASSURL', 'assesment_method', raw_course_data),
                    ('CRSEURL', 'course_page', raw_course_data),
                    ('EMPLOYURL', 'employment_details', raw_course_data),
                    ('SUPPORTURL', 'financial_support_details', raw_course_data),
                    ('LTURL', 'learning_and_teaching_methods', raw_course_data),
                    ('SUURL', 'student_union', raw_inst_data)]

    for item_detail in item_details:
        link_item = build_eng_welsh_item(item_detail[0], item_detail[2])
        if link_item:
            links[item_detail[1]] = link_item

    return links


def build_location_items(locations, locids, raw_inst_data, raw_course_data):
    """Returns a list of location items"""

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


def build_code_label_entry(lookup_table_raw_xml, lookup_table_local, key):
    """Generic function for building any code label entries"""
    entry = {}
    if key in lookup_table_raw_xml:
        code = lookup_table_raw_xml[key]
        entry['code'] = code
        entry['label'] = lookup_table_local[code]
    return entry


def build_qualification(lookup_table_raw_xml, kisaims):
    entry = {}
    if 'KISAIMCODE' in lookup_table_raw_xml:
        code = lookup_table_raw_xml['KISAIMCODE']
        label = kisaims.get_kisaim_label_for_key(code)
        entry['code'] = code
        if label:
            entry['label'] = label
    return entry


def build_course_entry(locations, locids, raw_inst_data, raw_course_data, kisaims):
    """builds the JSON document for a particular course"""
    outer_wrapper = {}
    outer_wrapper['id'] = get_uuid()
    outer_wrapper['created_at'] = datetime.datetime.utcnow().isoformat()
    outer_wrapper['version'] = 1

    course_entry = {}
    if 'UKPRNAPPLY' in raw_course_data:
        course_entry['application_provider'] = raw_course_data['UKPRNAPPLY']
    country_entry = build_country(raw_inst_data)
    if country_entry:
        course_entry['country'] = country_entry
    distance_learning = build_code_label_entry(
        raw_course_data, lookup.distance_learning_lookup, 'DISTANCE')
    if distance_learning:
        course_entry['distance_learning'] = distance_learning
    foundataion_year = build_code_label_entry(
        raw_course_data, lookup.foundation_year_availability, 'FOUNDATION')
    if foundataion_year:
        course_entry['foundation_year_availability'] = foundataion_year
    if 'HONOURS' in raw_course_data:
        course_entry['honours_award_provision'] = raw_course_data['HONOURS']
    course_entry['institution'] = build_institution(raw_inst_data)
    course_entry['kis_course_id'] = raw_course_data['KISCOURSEID']
    length_of_course = build_code_label_entry(
        raw_course_data, lookup.length_of_course, 'NUMSTAGE')
    if length_of_course:
        course_entry['length_of_course'] = length_of_course
    links = build_links(locations, locids, raw_inst_data, raw_course_data)
    if links:
        course_entry['links'] = links
    location_items = build_location_items(
        locations, locids, raw_inst_data, raw_course_data)
    if location_items:
        course_entry['locations'] = location_items
    mode = build_code_label_entry(raw_course_data, lookup.mode, 'KISMODE')
    if mode:
        course_entry['mode'] = mode
    nhs_funded = build_code_label_entry(
        raw_course_data, lookup.nhs_funded, 'NHS')
    if nhs_funded:
        course_entry['nhs_funded'] = nhs_funded
    qualification = build_qualification(raw_course_data, kisaims)
    if qualification:
        course_entry['qualification'] = qualification
    sandwich_year = build_code_label_entry(raw_course_data, lookup.sandwich_year, 'SANDWICH')
    if sandwich_year:
        course_entry['sandwich_year'] = sandwich_year
    title = build_eng_welsh_item('TITLE', raw_course_data)
    if title:
        course_entry['title'] = title
    if 'UCASPROGID' in raw_course_data:
        course_entry['ucas_code_id'] = raw_course_data['UCASPROGID']
    year_abroad = build_code_label_entry(raw_course_data, lookup.year_abroad, 'YEARABROAD')
    if year_abroad:
        course_entry['year_abroad'] = build_code_label_entry(raw_course_data, lookup.year_abroad, 'YEARABROAD')


    outer_wrapper['course'] = course_entry
    return outer_wrapper


#fname = "test-3-inst.xml"
fname = "../../test-files/kis.xml"
tree = ET.parse(fname)
root = tree.getroot()

kisaims = KisAims(root)
#print(kisaims.get_kisaim_data_for_key('021'))

locations = Locations(root)

course_count = 0
for institution in root.iter('INSTITUTION'):
    raw_inst_data = xmltodict.parse(
        ET.tostring(institution))['INSTITUTION']
    ukprn = raw_inst_data['UKPRN']
    for course in institution.findall('KISCOURSE'):
        raw_course_data = xmltodict.parse(ET.tostring(course))['KISCOURSE']
        """Change location to a list and just pass this rather than location data"""
        locids = get_locids(raw_course_data, ukprn)
        course_entry = build_course_entry(
            locations, locids, raw_inst_data, raw_course_data, kisaims)
        print(json.dumps(course_entry, indent=4))
        course_count += 1


print(f'Processed {course_count} courses')
