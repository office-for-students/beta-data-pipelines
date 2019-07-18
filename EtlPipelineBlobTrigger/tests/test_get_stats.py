import json
import os
import unittest
import xml.etree.ElementTree as ET

import xmltodict

import course_stats

def get_string(filename):
    """Reads file into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestGetStats(unittest.TestCase):

    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string('fixtures/large-test-file.xml')
        root = ET.fromstring(xml_string)
        for institution in root.iter('INSTITUTION'):
            for course in institution.findall('KISCOURSE'):
                raw_course_data = xmltodict.parse(
                    ET.tostring(course))['KISCOURSE']
                course_stats.get_stats(raw_course_data)

# TODO Test more of the functionality

if __name__ == '__main__':
    unittest.main()
