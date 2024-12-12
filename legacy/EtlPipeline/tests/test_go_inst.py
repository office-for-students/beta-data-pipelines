import json
import unittest
from unittest import mock

import defusedxml.ElementTree as ET
import xmltodict

from legacy.EtlPipeline.course_docs import get_go_inst_json
from legacy.EtlPipeline.tests.test_helpers.testing_utils import get_string


class TestGetGoInst(unittest.TestCase):

    @mock.patch("EtlPipeline.subject_enricher.SubjectCourseEnricher")
    def test_get_go_inst_no_data(self, subject_enricher):
        xml_string = get_string("fixtures/specific_institution.xml")
        xml_root = ET.fromstring(xml_string)
        element = "GOSALARY"

        for course in xml_root.iter("KISCOURSE"):
            raw_course_xml = xmltodict.parse(ET.tostring(course))["KISCOURSE"][element]
            expected_response = json.loads(
                get_string("fixtures/course_go_inst_no_data_resp.json")
            )

            json_obj = get_go_inst_json(raw_course_xml, subject_enricher)
        print(json_obj)

        # self.assertEqual(json_obj[0], expected_response[0])
