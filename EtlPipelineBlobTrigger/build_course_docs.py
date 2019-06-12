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

from locations import Locations


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


def build_foundation_year_availability(raw_inst_data, raw_course_data):
    code = raw_course_data['FOUNDATION']
    foundation_year_availability = {
        '0': 'Not available',
        '1': 'Optional',
        '2': 'Compulsory',
    }

    code = raw_course_data['FOUNDATION']
    label = foundation_year_availability[code]

    return {'code': code, 'label': label}


def build_length_of_course(raw_course_data):
    length_of_course_lookup = {
        '1': '1 stage',
        '2': '2 stages',
        '3': '3 stages',
        '4': '4 stages',
        '5': '5 stages',
        '6': '6 stages',
        '7': '7 stages',
    }

    code = raw_course_data['NUMSTAGE']
    label = length_of_course_lookup[code]
    return {'code': code, 'label': label}


def build_distance_learning(raw_inst_data, raw_course_data):
    distance_learning_lookup = {
        '0': 'Course is availble other than by distance learning',
        '1': 'Course is only availble through distance learning',
        '2': 'Course is optionally available through distance learning',
    }

    code = raw_course_data['DISTANCE']

    return {
        'code': raw_course_data['DISTANCE'],
        'label': distance_learning_lookup[code],
    }


def build_country(raw_inst_data):
    country_code_lookup = {
        'XF': 'England',
        'XG': 'Northern Ireland',
        'XH': 'Scotland',
        'XI': 'Wales',
    }

    code = raw_inst_data['COUNTRY']
    return {'code': code, 'name': country_code_lookup[code]}


def get_locids(raw_course_data):
    """Return a list of location IDs"""
    locids = []
    if 'COURSELOCATION' not in raw_course_data:
        return locids
    if type(raw_course_data['COURSELOCATION']) == list:
        for val in raw_course_data['COURSELOCATION']:
            # TODO check if UCASCOURSEIDs could be in here
            # if so need skip over so wil
            try:
                locids.append(val['LOCID'])
            except KeyError:
                # Handle COURSELOCATION without LOCID - see KISCOURSEID BADE for an example
                # Distant learning my provide a UCASCOURSEID
                pass
    else:
        try:
            locids.append(raw_course_data['COURSELOCATION']['LOCID'])
        except KeyError:
            # Handle COURSELOCATION without LOCID - see KISCOURSEID BADE for an example
            pass
    return locids

def build_accomodation(locations, locids):
    """Needs to return a list"""
    accom = []
    for locid in locids:
        accom_dict = {}
        raw_location_data = locations.get_location_data_for_locid(locid)
        if 'ACCOMURL' in raw_location_data:
            accom_dict['english'] = raw_location_data['ACCOMURL']
        if 'ACCOMURLW' in raw_location_data:
            accom_dict['welsh'] = raw_location_data['ACCOMURLW']
        if accom_dict:
            accom.append(accom_dict)
    return accom

def build_assessment_method(raw_course_data):
    assessment_method = {}
    if 'ASSURL' in raw_course_data:
        assessment_method['english'] = raw_course_data['ASSURL']
    if 'ASSURLW' in raw_course_data:
        assessment_method['welsh'] = raw_course_data['ASSURLW']
    return assessment_method

def build_course_page(raw_course_data):
    course_page = {}
    if 'CRSEURL' in raw_course_data:
        course_page['english'] = raw_course_data['CRSEURL']
    if 'CRSEURLW' in raw_course_data:
        course_page['welsh'] = raw_course_data['CRSEURLW']
    return course_page

def build_employment_details(raw_course_data):
    employment_details = {}
    if 'EMPLOYURL' in raw_course_data:
        employment_details['english'] = raw_course_data['EMPLOYURL']
    if 'EMPLOYURLW' in raw_course_data:
        employment_details['welsh'] = raw_course_data['EMPLOYURLW']
    return employment_details

def build_links(locations, locids, raw_inst_data, raw_course_data):
    links = {}
    if locids:
        accomodation = build_accomodation(locations, locids)
        links['accomodation'] = accomodation
    assessment_method = build_assessment_method(raw_course_data)
    if assessment_method:
        links['assessment_method'] = assessment_method
    course_page = build_course_page(raw_course_data)
    if course_page:
        links['course_page'] = course_page
    employment_details = build_employment_details(raw_course_data)
    if employment_details:
        links['employment_details'] = employment_details
    return links

def build_course_entry(locations, locids, raw_inst_data, raw_course_data):
    outer_wrapper = {}
    outer_wrapper['id'] = get_uuid()
    outer_wrapper['created_at'] = datetime.datetime.utcnow().isoformat()
    outer_wrapper['version'] = 1

    course_entry = {}
    if 'UKPRNAPPLY' in raw_course_data:
        course_entry['application_provider'] = raw_course_data['UKPRNAPPLY']
    if 'COUNTRY' in raw_inst_data:
        course_entry['country'] = build_country(raw_inst_data)
    if 'DISTANCE' in raw_course_data:
        course_entry['distance_learning'] = build_distance_learning(
            raw_inst_data, raw_course_data)
    if 'FOUNDATION' in raw_course_data:
        course_entry['foundation_year_availability'] = build_foundation_year_availability(
            raw_inst_data, raw_course_data)
    if 'HONOURS' in raw_course_data:
        course_entry['honours_award_provision'] = raw_course_data['HONOURS']
    course_entry['institution'] = build_institution(raw_inst_data)
    course_entry['kis_course_id'] = raw_course_data['KISCOURSEID']
    if 'NUMSTAGE' in raw_course_data:
        course_entry['length_of_course'] = build_length_of_course(
            raw_course_data)
    links = build_links(locations, locids, raw_inst_data, raw_course_data)
    if links:
        course_entry['links'] = links
    outer_wrapper['course'] = course_entry
    return outer_wrapper

#fname = "../test-data/test-3-inst.xml"
fname = "kis.xml"
tree = ET.parse(fname)
root = tree.getroot()

locations = Locations(root)

course_count = 0
for institution in root.iter('INSTITUTION'):
    raw_inst_data = xmltodict.parse(
        ET.tostring(institution))['INSTITUTION']
    for course in institution.findall('KISCOURSE'):
        raw_course_data = xmltodict.parse(ET.tostring(course))['KISCOURSE']
        """Change location to a list and just pass this rather than location data"""
        locids = get_locids(raw_course_data)
        #location = locations.get_location_data_for_locid(locid)
        course_entry = build_course_entry(
            locations, locids, raw_inst_data, raw_course_data)
        print(json.dumps(course_entry, indent=4))
        course_count += 1


print(f'Processed {course_count} courses')
