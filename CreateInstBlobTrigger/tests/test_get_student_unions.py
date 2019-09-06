import json
import unittest
import defusedxml.ElementTree as ET

from inst_test_utils import get_first, get_string
from CreateInstBlobTrigger.locations import Locations
from institution_docs import get_student_unions


class TestGetStudentUnions(unittest.TestCase):
    def test_get_student_unions_one_course(self):
        kis_xml_string = get_string("fixtures/large_test_file.xml")
        inst_xml_string = get_string("fixtures/one_inst_one_course.xml")
        kis_root = ET.fromstring(kis_xml_string)
        locations = Locations(kis_root)
        inst_root = ET.fromstring(inst_xml_string)
        institution = get_first(inst_root.iter("INSTITUTION"))
        get_student_unions(locations, institution)

    def test_get_student_unions_nine_courses(self):

        # Build a locations lookup
        kis_xml_string = get_string("fixtures/large_test_file.xml")
        kis_root = ET.fromstring(kis_xml_string)
        location_lookup = Locations(kis_root)

        # Get an institution so we can pass it to get_student_unions
        institution_xml_string = get_string(
            "fixtures/one_inst_nine_courses.xml"
        )
        institution_root = ET.fromstring(institution_xml_string)
        institution = get_first(institution_root.iter("INSTITUTION"))

        student_unions = get_student_unions(location_lookup, institution)
        expected_student_unions = json.loads(
            get_string("fixtures/one_inst_nine_courses.json")
        )
        self.assertEqual(expected_student_unions, student_unions)

    def test_get_student_unions_413_courses(self):

        # Build a locations lookup
        kis_xml_string = get_string("fixtures/large_test_file.xml")
        kis_root = ET.fromstring(kis_xml_string)
        location_lookup = Locations(kis_root)

        # Get an institution so we can pass it to get_student_unions
        institution_xml_string = get_string(
            "fixtures/one_inst_413_courses.xml"
        )
        institution_root = ET.fromstring(institution_xml_string)
        institution = get_first(institution_root.iter("INSTITUTION"))

        student_unions = get_student_unions(location_lookup, institution)
        expected_student_unions = json.loads(
            get_string("fixtures/one_inst_413_courses.json")
        )
        self.assertEqual(expected_student_unions, student_unions)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
