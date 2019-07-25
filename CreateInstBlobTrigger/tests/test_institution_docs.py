import json
import unittest

import xml.etree.ElementTree as ET

import xmltodict

from institution_docs import get_total_number_of_courses
from course_stats import Leo

from testing_utils import get_string


class TestStaticHelperFunctions(unittest.TestCase):
    def test_get_total_number_of_courses(self):

        def get_first(Node):
            for x in Node:
                return x

        xml_string = get_string("fixtures/one_inst_one_course.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        expected_number_of_courses = 1
        number_of_courses = get_total_number_of_courses(institution)
        self.assertEqual(expected_number_of_courses, number_of_courses)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
