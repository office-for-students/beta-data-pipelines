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

    def test_get_tariff_description(self):
        self.assertEqual(self.tariff.get_tariff_description('T001'), 'less than 48 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T048'), 'between 48 and 63 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T064'), 'between 64 and 79 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T080'), 'between 80 and 95 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T096'), 'between 96 and 111 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T208'), 'between 208 and 223 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T224'), 'between 224 and 239 tariff points')
        self.assertEqual(self.tariff.get_tariff_description('T240'), '240 or more tariff points')

    def test_get_tariffs_list(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_tariff_with_subj.xml'))['KISCOURSE']
        raw_xml_list = SharedUtils.get_raw_list(raw_course_xml, 'TARIFF')
        tariff_xml_elem = raw_xml_list[0]
        tariff_list = self.tariff.get_tariffs_list(tariff_xml_elem)
        expected_result = json.loads(
            get_string('fixtures/tariff_get_tariffs_list_resp.json'))
        self.assertListEqual(tariff_list, expected_result)

class TestGetStats(unittest.TestCase):
    def setUp(self):
        self.tariff = Tariff()

    def test_get_stats_with_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string('fixtures/course_tariff_with_subj.xml'))['KISCOURSE']
        expected_result = json.loads(
            get_string('fixtures/tariff_get_stats_resp_with_subj.json'))
        json_obj = self.tariff.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_result)


# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
