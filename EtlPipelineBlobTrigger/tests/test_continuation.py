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

from course_stats import Continuation


def get_string(filename):
    """Reads file in test dir into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestGetContinuationKey(unittest.TestCase):
    def setUp(self):
        self.continuation = Continuation()

    def test_with_valid_key(self):
        expected_key = 'number_of_students'
        xml_key = 'CONTPOP'
        key = self.continuation.get_continuation_key(xml_key)
        self.assertEqual(expected_key, key)

    def test_with_valid_key(self):
        invalid_xml_key = 'invalid_key'
        with self.assertRaises(KeyError):
            self.continuation.get_continuation_key(invalid_xml_key)


class TestGetContinuation(unittest.TestCase):
    def setUp(self):
        self.continuation = Continuation()


    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string('fixtures/large-test-file.xml')
        root = ET.fromstring(xml_string)
        for institution in root.iter('INSTITUTION'):
            for course in institution.findall('KISCOURSE'):
                raw_course_data = xmltodict.parse(ET.tostring(course))['KISCOURSE']
                continuation = self.continuation.get_continuation(raw_course_data)

    def test_get_continuation_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_no_cont_subj.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_no_cont_subj_resp.json'))
        continuation = self.continuation.get_continuation(raw_course_xml)
        self.assertListEqual(continuation, expected_response)

    def test_get_continuation_two_continuations_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_two_contins.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_two_contins_resp.json'))
        continuation = self.continuation.get_continuation(raw_course_xml)
        self.assertListEqual(continuation, expected_response)

    def test_get_continuation_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_cont_subj.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_cont_subj_resp.json'))
        continuation = self.continuation.get_continuation(raw_course_xml)
        self.assertListEqual(continuation, expected_response)

    def test_get_continuation_agg_14(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_532986.xml'))['KISCOURSE']
        expected_response = json.loads(
            get_string('fixtures/course_532986_resp.json'))
        continuation = self.continuation.get_continuation(raw_course_xml)
        self.assertListEqual(continuation, expected_response)

 # TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
