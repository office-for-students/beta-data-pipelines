import inspect
import json
import os
import sys
import unittest
import xml.etree.ElementTree as ET

import xmltodict

from course_stats import SharedUtils, Tariff

def get_string(filename):
    """Reads file into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestVariousHelperMethods(unittest.TestCase):
    def setUp(self):
        self.tariff = Tariff()

    def test_is_tariff(self):
        self.assertTrue(self.tariff.is_tariff('T001'))
        self.assertTrue(self.tariff.is_tariff('T048'))
        self.assertFalse(self.tariff.is_tariff('T999'))

    def test_get_tariff_description(self):
        self.assertEqual(self.tariff.get_tariff_description('T001'), 'less than 48 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T208'), 'between 208 and 239 tariff points')

    def test_get_tariff_list(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_tariff_with_subj.xml'))['KISCOURSE']
        raw_xml_list = SharedUtils.get_raw_list(raw_course_xml, 'TARIFF')
        tariff_xml_elem = raw_xml_list[0]
        tariff_list = self.tariff.get_tariff_list(tariff_xml_elem)
        expected_result = json.loads(
            get_string('fixtures/course_tariff_with_subj.json'))
        self.assertListEqual(tariff_list, expected_result)
        print(json.dumps(tariff_list, indent=4))


# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
