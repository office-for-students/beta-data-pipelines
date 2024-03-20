import unittest

import defusedxml.ElementTree as ET
import xmltodict

from legacy.EtlPipeline.course_stats import need_nhs_nss
from legacy.EtlPipeline.tests.test_helpers.testing_utils import get_first
from legacy.EtlPipeline.tests.test_helpers.testing_utils import get_string


class TestNeedNhsNss(unittest.TestCase):
    def test_with_large_file(self):
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        for institution in root.iter("INSTITUTION"):
            raw_inst_data = xmltodict.parse(ET.tostring(institution))[
                "INSTITUTION"
            ]
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))[
                    "KISCOURSE"
                ]
                need_nhs_nss(raw_course_data)

    def test_need_nhs_nss_one_inst_one_course(self):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        course = get_first(institution.findall("KISCOURSE"))
        raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
        self.assertFalse(need_nhs_nss(raw_course_data))

    def test_need_nhs_nss_with_nhs_nss(self):
        course_xml = xmltodict.parse(
            get_string("fixtures/course_nhs_subj.xml")
        )["KISCOURSE"]
        self.assertTrue(need_nhs_nss(course_xml))


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
