import json
import unittest
import xml.etree.ElementTree as ET

from unittest import mock

from inst_test_utils import get_first, get_string, remove_variable_elements
from institution_docs import (
    InstitutionDocs,
    get_country,
    get_total_number_of_courses,
)


class TestStaticHelperFunctions(unittest.TestCase):
    def test_get_total_number_of_courses_with_one_course(self):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        expected_number_of_courses = 1
        number_of_courses = get_total_number_of_courses(institution)
        self.assertEqual(expected_number_of_courses, number_of_courses)

    def test_get_total_number_of_courses_with_nine_courses(self):
        xml_string = get_string("fixtures/one_inst_nine_courses.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        expected_number_of_courses = 9
        number_of_courses = get_total_number_of_courses(institution)
        self.assertEqual(expected_number_of_courses, number_of_courses)

    def test_get_country_england(self):
        expected_resp = json.loads(get_string("fixtures/country_england.json"))
        code = "XF"
        resp = get_country(code)
        self.assertEqual(expected_resp, resp)

    def test_get_country_wales(self):
        expected_resp = json.loads(get_string("fixtures/country_wales.json"))
        code = "XI"
        resp = get_country(code)
        self.assertEqual(expected_resp, resp)

    def test_get_country_scotland(self):
        expected_resp = json.loads(
            get_string("fixtures/country_scotland.json")
        )
        code = "XH"
        resp = get_country(code)
        self.assertEqual(expected_resp, resp)

    def test_get_country_ni(self):
        expected_resp = json.loads(get_string("fixtures/country_ni.json"))
        code = "XG"
        resp = get_country(code)
        self.assertEqual(expected_resp, resp)


class TestGetInstitionDoc(unittest.TestCase):
    @mock.patch("institution_docs.utils")
    def setUp(self, mock_utils):
        mock_ukrlp_lookup = json.loads(
            get_string("fixtures/mock_ukrlp_lookup.json")
        )
        mock_utils.get_ukrlp_lookups.return_value = mock_ukrlp_lookup
        self.institution_docs = InstitutionDocs()

    def test_get_institution_doc(self):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        expected_resp = json.loads(
            get_string("fixtures/one_inst_one_course.json")
        )
        expected_resp = remove_variable_elements(expected_resp)
        resp = dict(self.institution_docs.get_institution_doc(institution))
        resp = remove_variable_elements(resp)
        self.assertEqual(expected_resp, resp)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
