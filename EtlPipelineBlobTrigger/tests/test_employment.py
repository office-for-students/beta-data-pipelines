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

from course_stats import Continuation, Employer


def get_string(filename):
    """Reads file in test dir into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestGetEmployerKey(unittest.TestCase):
    def setUp(self):
        self.continuation = Continuation()

    def test_with_valid_key(self):
        expected_key = 'number_of_students'
        xml_key = 'EMPPOP'
        key = self.continuation.get_continuation_key(xml_key)
        self.assertEqual(expected_key, key)

    def test_with_valid_key(self):
        invalid_xml_key = 'invalid_key'
        with self.assertRaises(KeyError):
            self.continuation.get_continuation_key(invalid_xml_key)


class TestGetEmployer(unittest.TestCase):
    def setUp(self):
        self.employment = Employer()


    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string('fixtures/large-test-file.xml')
        root = ET.fromstring(xml_string)
        for institution in root.iter('INSTITUTION'):
            for course in institution.findall('KISCOURSE'):
                raw_course_data = xmltodict.parse(ET.tostring(course))['KISCOURSE']
                employer = self.employment.get_employment(raw_course_data)
                print(json.dumps(employer, indent=4))


 # TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
