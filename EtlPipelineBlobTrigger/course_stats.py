import datetime
import json
import logging
import pprint
import uuid
import os
import sys

import azure.cosmos.cosmos_client as cosmos_client
import xmltodict
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.DEBUG)


def get_continuation_key(xml_key):

    return {
        'CONTUNAVAILREASON':'unavailable',
        "CONTPOP": 'number_of_students',
        "CONTAGG": 'aggregation_level',
        "CONTSBJ": 'subject',
        "UCONT": 'continuing_with_provider',
        "UDORMANT": 'dormant',
        "UGAINED": 'gained',
        "ULEFT": 'left',
        "ULOWER": 'lower',
    }[xml_key]

def get_english_contsbj_label(code):
    pass

def get_english_contsbj_label(code):
    pass

def get_continuation_subject(xml_dict):
    subject = {}
    pp = pprint.PrettyPrinter(indent=4)
    code = xml_dict['CONTSBJ']
    subject['code'] = code
    subject['english_label'] = get_english_label(code)
    subject['welsh_label'] = get_welsh_label(code)
    pp.pprint(subject)



def get_continuation(raw_course_data):
    continuation = {}
    xml_dict = raw_course_data['CONTINUATION']
    for xml_key in xml_dict:
        json_key = get_continuation_key(xml_key)
        if json_key == 'subject':
            continuation[json_key] = get_continuation_subject(xml_dict)
        elif json_key == 'unavailable':
            continuation[json_key] = {'TODO':'TODO'}
        else:
            continuation[json_key] = xml_dict[xml_key]
    return continuation


def build_stats(raw_course_data):
    stats = {}

    continuation = get_continuation(raw_course_data)
    stats['continuation'] = continuation
    return stats


def test_build_stats():
    """Function for testing LookupCreator"""

    # Change the open line as required during test/debug
    with open('../test-data/kis-short.xml', 'r') as file:
        xml_string = file.read()

    root = ET.fromstring(xml_string)

    course_count = 0
    for institution in root.iter('INSTITUTION'):
        raw_inst_data = xmltodict.parse(
            ET.tostring(institution))['INSTITUTION']
        ukprn = raw_inst_data['UKPRN']
        for course in institution.findall('KISCOURSE'):
            raw_course_data = xmltodict.parse(ET.tostring(course))['KISCOURSE']
            stats_entry = build_stats(raw_course_data)
            course = {}
            course['stats'] = stats_entry
            pp = pprint.PrettyPrinter(indent=4)
            #pp.pprint(course)
            course_count += 1
    logging.info(f"Processed {course_count} courses")


test_build_stats()
