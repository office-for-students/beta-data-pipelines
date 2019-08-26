import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from testing_utils import get_first, get_string, remove_variable_elements
from course_docs import get_course_doc, get_locids
from accreditations import Accreditations
from locations import Locations
from kisaims import KisAims
from testing_utils import get_first, get_string


class TestGetCourseDoc(unittest.TestCase):

    def test_with_large_file(self):
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        accreditations = Accreditations(root)
        kisaims = KisAims(root)
        locations = Locations(root)
        for institution in root.iter("INSTITUTION"):
            raw_inst_data = xmltodict.parse(ET.tostring(institution))[
                "INSTITUTION"
            ]
            ukprn = raw_inst_data["UKPRN"]
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))[
                    "KISCOURSE"
                ]
                locids = get_locids(raw_course_data, ukprn)
                course_doc = get_course_doc(
                    accreditations,
                    locations,
                    locids,
                    raw_inst_data,
                    raw_course_data,
                    kisaims,
                )


    def test_specific_10000047(self):
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        accreditations = Accreditations(root)
        kisaims = KisAims(root)
        locations = Locations(root)
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        raw_inst_data = xmltodict.parse(ET.tostring(institution))[ "INSTITUTION"]
        ukprn = raw_inst_data["UKPRN"]
        course = get_first(institution.findall("KISCOURSE"))
        raw_course_data = xmltodict.parse(ET.tostring(course))[ "KISCOURSE" ]
        locids = get_locids(raw_course_data, ukprn)
        expected_course_doc = json.loads(get_string("fixtures/one_inst_one_course.json"))
        course_doc = get_course_doc(
            accreditations,
            locations,
            locids,
            raw_inst_data,
            raw_course_data,
            kisaims,
        )
        course_doc = remove_variable_elements(course_doc)
        self.assertEqual(expected_course_doc, course_doc)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
