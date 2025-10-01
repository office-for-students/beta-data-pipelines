import unittest

import defusedxml.ElementTree as ET

from institution_creation.locations import Locations
from institution_creation.tests.test_helpers.inst_test_utils import get_string


class TestLocationLookup(unittest.TestCase):
    def test_get_location(self):
        kis_xml_string = get_string("fixtures/large_test_file.xml")
        kis_root = ET.fromstring(kis_xml_string)
        locations = Locations(kis_root)
        institution_pubukprn = "10000047"
        locid = "A1"
        lookup_key = f"{institution_pubukprn}{locid}"
        location_data = locations.get_location(lookup_key)
        self.assertEqual(location_data["UKPRN"], "10001143")
        self.assertEqual(location_data["LOCID"], "A1")
        self.assertEqual(location_data["LOCUKPRN"], "10000047")
        self.assertEqual(location_data["LOCCOUNTRY"], "XF")
        self.assertEqual(location_data["SUURL"], "http://ccsu.co.uk/")


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
