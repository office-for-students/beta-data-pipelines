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

import sys # debug onlu
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

    return {'code': code, 'label':label}

def build_length_of_course(raw_course_data):
    length_of_course_lookup = {
        '0': 'todo',
        '1': 'todo',
        '2': 'todo',
        '3': 'todo',
        '4': 'todo',
        '5': 'todo',
        '6': 'todo',
        '7': 'todo',
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

    distance_learning = {
        'code': raw_course_data['DISTANCE'],
        'label': distance_learning_lookup[code],
    }
    return distance_learning


def build_course(raw_inst_data, raw_course_data):
    country_code_lookup = {
        'XF': 'England',
        'XG': 'Northern Ireland',
        'XH': 'Scotland',
        'XI': 'Wales',
    }

    course_section = {}

    # Need to check as Schema states this is optional
    if 'UKPRNAPPLY' in raw_course_data:
        course_section['application_provider'] = raw_course_data['UKPRNAPPLY']

    country_code = raw_inst_data['COUNTRY']
    country_name = country_code_lookup[country_code]
    course_section['country'] = {'code': country_code, 'name': country_name}
    return course_section


def build_output_course(raw_inst_data, raw_course_data):
    output_course = {}
    output_course['id'] = get_uuid()
    output_course['created_at'] = datetime.datetime.utcnow().isoformat()
    output_course['version'] = 1

    output_course['course'] = build_course(raw_inst_data, raw_course_data)
    if 'DISTANCE' in raw_course_data:
        output_course['distance_learning'] = build_distance_learning(raw_inst_data, raw_course_data)
    if 'FOUNDATION' in raw_course_data:
        output_course['foundation_year_availability'] = build_foundation_year_availability(raw_inst_data, raw_course_data)
    if 'HONOURS' in raw_course_data:
        output_course['honours_award_provision'] = raw_course_data['HONOURS']
    output_course['institution'] = build_institution(raw_inst_data)
    output_course['kis_course_id'] = raw_course_data['KISCOURSEID']
    if 'NUM_STAGE' in raw_course_data:
        output_course['length_of_course'] = build_length_of_course(raw_course_data)
    return output_course


#fname = "../test-data/test-3-inst.xml"
fname = "kis.xml"
tree = ET.parse(fname)
root = tree.getroot()

course_count = 0
for institution in root.iter('INSTITUTION'):
    raw_inst_data = xmltodict.parse(ET.tostring(institution))['INSTITUTION']
    for course in institution.findall('KISCOURSE'):
        raw_course_data = xmltodict.parse(ET.tostring(course))['KISCOURSE']
        output_course = build_output_course(raw_inst_data, raw_course_data)
        print(json.dumps(output_course, indent=4))
        course_count += 1
        #if course_count > 10:
        #    break

print(f'Processed {course_count} courses')
