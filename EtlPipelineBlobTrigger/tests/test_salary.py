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


class TestLookupDataFields(unittest.TestCase):
    def setUp(self):
        self.salary = Salary()
        self.lookup = self.salary.salary_data_fields_lookup

    def test_salpop_lookup(self):
        xml_key = 'SALPOP'
        expected_key = 'number_of_graduates'
        expected_elem_type = 'M'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

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

    def test_salary_get_stats_with_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_sal_subj.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_sal_subj_resp.json'))
        json_obj = self.salary.get_stats(raw_course_xml)
        print(json.dumps(json_obj, indent=4))
        self.assertListEqual(json_obj, expected_response)

# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
