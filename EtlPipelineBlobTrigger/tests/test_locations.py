import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict


from course_docs import get_location_items, get_locids
from locations import Locations
from testing_utils import get_string


class TestGetLocations(unittest.TestCase):
    def setUp(self):
        xml_string = get_string("fixtures/course_with_locations.xml")
        root = ET.fromstring(xml_string)
        self.locations = Locations(root)

    def test_get_multiple_locations(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_with_locations.xml")
        )["KIS"]["KISCOURSE"]
        pub_ukprn = "10007814"
        locids = get_locids(raw_course_xml, pub_ukprn)

        expected_response = json.loads(get_string("fixtures/course_location_resp.json"))
        locations = get_location_items(
            self.locations, locids, raw_course_xml, pub_ukprn
        )

        self.assertListEqual(locations, expected_response)
