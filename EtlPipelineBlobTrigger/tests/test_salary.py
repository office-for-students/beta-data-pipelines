import inspect
import json
import os
import sys
import unittest
import xml.etree.ElementTree as ET

import xmltodict

from course_stats import Salary


def get_string(filename):
    """Reads file in test dir into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestGetSalaryKey(unittest.TestCase):
    def setUp(self):
        self.salary = Salary()

    def test_with_valid_key(self):
        expected_key = 'number_of_graduates'
        xml_key = 'SALPOP'
        key = self.salary.get_key(xml_key)
        self.assertEqual(expected_key, key)

    def test_with_invalid_key(self):
        # TODO change this to test for KeyError exception when
        # all Salary fields mapped.
        invalid_xml_key = 'invalid_key'
        key = self.salary.get_key(invalid_xml_key)
        self.assertIsNone(key)


class TestGetSalary(unittest.TestCase):
    def setUp(self):
        self.salary = Salary()

    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string('fixtures/large-test-file.xml')
        root = ET.fromstring(xml_string)
        for institution in root.iter('INSTITUTION'):
            for course in institution.findall('KISCOURSE'):
                raw_course_data = xmltodict.parse(
                    ET.tostring(course))['KISCOURSE']
                self.salary.get_stats(raw_course_data)

    def test_get_stats_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_no_subj_for_most.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_no_sal_subj_resp.json'))
        json_obj = self.salary.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)

    def test_get_stats_with_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_sal_subj.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_sal_subj_resp.json'))
        json_obj = self.salary.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)

# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
