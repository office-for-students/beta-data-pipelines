import json
import os
import unittest
import xml.etree.ElementTree as ET

import xmltodict

from course_docs import get_location_items, get_locids
from locations import Locations


def get_string(filename):
    """Reads file in test dir into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestGetLocations(unittest.TestCase):
    def setUp(self):
        xml_string = get_string('fixtures/course_with_locations.xml')
        root = ET.fromstring(xml_string)
        self.locations = Locations(root)
        self.maxDiff = None

    def test_get_stats_subj(self):
        raw_course_xml = xmltodict.parse(get_string(
            'fixtures/course_with_locations.xml'))['KIS']['KISCOURSE']
        pub_ukprn = '10007814'
        locids = get_locids(raw_course_xml, pub_ukprn)

        expected_response = json.loads(get_string(
            'fixtures/course_location_resp.json'))
        locations = get_location_items(self.locations, locids,
                                       raw_course_xml, pub_ukprn)

        self.assertListEqual(locations, expected_response)
