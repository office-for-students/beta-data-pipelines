import inspect
import json
import os
import sys
import unittest
import xml.etree.ElementTree as ET

import xmltodict
from course_stats import Nss

from dictdiffer import diff #remove

def get_string(filename):
    """Reads file in test dir into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string

class TestNss(unittest.TestCase):
    def setUp(self):
        self.nss = Nss()

    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string('fixtures/large-test-file.xml')
        root = ET.fromstring(xml_string)
        for institution in root.iter('INSTITUTION'):
            for course in institution.findall('KISCOURSE'):
                raw_course_data = xmltodict.parse(
                    ET.tostring(course))['KISCOURSE']
                self.nss.get_stats(raw_course_data)


    def test_get_stats_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_nss_questions.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_nss_questions_resp.json'))
        json_obj = self.nss.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)


    def test_NSS_get_stats_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_nss_subj.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_nss_subj_resp.json'))
        json_obj = self.nss.get_stats(raw_course_xml)
        debug_res = diff(expected_response, json_obj)
        print(list(debug_res))
        self.assertListEqual(json_obj, expected_response)


# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
