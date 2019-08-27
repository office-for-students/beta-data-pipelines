"""Test the course Salary statistics"""
import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from course_stats import Salary
from testing_utils import get_string


class TestLookupDataFields(unittest.TestCase):
    def setUp(self):
        self.salary = Salary()
        self.lookup = self.salary.salary_data_fields_lookup

    def test_salagg_lookup(self):
        xml_key = 'SALAGG'
        expected_key = 'aggregation_level'
        expected_elem_type = 'M'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_salresp_rate_lookup(self):
        xml_key = 'SALRESP_RATE'
        expected_key = 'response_rate'
        expected_elem_type = 'M'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_salpop_lookup(self):
        xml_key = 'SALPOP'
        expected_key = 'number_of_graduates'
        expected_elem_type = 'M'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_sal_subj_lookup(self):
        xml_key = 'SALSBJ'
        expected_key = 'subject'
        expected_elem_type = 'O'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_lq_lookup(self):
        xml_key = 'LQ'
        expected_key = 'sector_lower_quartile'
        expected_elem_type = 'O'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_uq_lookup(self):
        xml_key = 'UQ'
        expected_key = 'sector_higher_quartile'
        expected_elem_type = 'O'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_med_lookup(self):
        xml_key = 'MED'
        expected_key = 'sector_median'
        expected_elem_type = 'O'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_instlq_lookup(self):
        xml_key = 'INSTLQ'
        expected_key = 'lower_quartile'
        expected_elem_type = 'M'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_instmed_lookup(self):
        xml_key = 'INSTMED'
        expected_key = 'median'
        expected_elem_type = 'M'
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_instuq_lookup(self):
        xml_key = 'INSTUQ'
        expected_key = 'higher_quartile'
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
        self.assertListEqual(json_obj, expected_response)


# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
