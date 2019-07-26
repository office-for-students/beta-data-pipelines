import json
import unittest

import xml.etree.ElementTree as ET

from institution_docs import get_total_number_of_courses, get_country

from inst_test_utils import get_string, get_first


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
        expected_resp = json.loads(
            get_string("fixtures/country_england.json")
        )
        code = 'XF'
        resp = get_country(code)
        #print(json.dumps(country, indent=4))
        self.assertEqual(expected_resp, resp)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
