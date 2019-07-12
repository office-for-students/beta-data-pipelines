import inspect
import json
import os
import sys
import unittest
import xml.etree.ElementTree as ET

import xmltodict

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from course_stats import Entry


def get_string(filename):
    """Reads file in test dir into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestGetEntryKey(unittest.TestCase):
    def setUp(self):
        self.entry = Entry()

    def test_with_valid_key(self):
        expected_key = 'number_of_students'
        xml_key = 'ENTPOP'
        key = self.entry.get_key(xml_key)
        self.assertEqual(expected_key, key)

    def test_with_invalid_key(self):
        invalid_xml_key = 'invalid_key'
        with self.assertRaises(KeyError):
            self.entry.get_key(invalid_xml_key)


class TestGetEntry(unittest.TestCase):
    def setUp(self):
        self.entry = Entry()

    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string('fixtures/large-test-file.xml')
        root = ET.fromstring(xml_string)
        for institution in root.iter('INSTITUTION'):
            for course in institution.findall('KISCOURSE'):
                raw_course_data = xmltodict.parse(
                    ET.tostring(course))['KISCOURSE']
                self.entry.get_entry(raw_course_data)

    def test_get_entry_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_no_subj_for_most.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_no_entry_subj_resp.json'))
        json_obj = self.entry.get_entry(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)

    def test_get_entry_with_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_with_subj_for_most.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_entry_subj_resp.json'))
        json_obj = self.entry.get_entry(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)


# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
