import json
import unittest
import defusedxml.ElementTree as ET

from inst_test_utils import get_string
from CreateInst.locations import Locations
from institution_docs import get_student_union


class TestGetStudentUnion(unittest.TestCase):
    def test_get_student_union_10000047(self):
        kis_xml_string = get_string("fixtures/large_test_file.xml")
        kis_root = ET.fromstring(kis_xml_string)
        locations_lookup = Locations(kis_root)
        institution_pubukprn = "10000047"
        locid = "A1"
        lookup_key = f"{institution_pubukprn}{locid}"
        location = locations_lookup.get_location(lookup_key)
        student_union = get_student_union(location)
        expected_student_union = json.loads(
            get_string("fixtures/su_10000047.json")
        )
        self.assertEqual(expected_student_union, student_union)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
