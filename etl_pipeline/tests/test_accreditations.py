import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from etl_pipeline.lookups.accreditations import Accreditations
from etl_pipeline.course_docs import get_accreditations
from etl_pipeline.tests.test_helpers.testing_utils import get_string


class TestGetAccreditations(unittest.TestCase):
    def setUp(self):
        xml_string = get_string("fixtures/course_with_accreditations.xml")
        root = ET.fromstring(xml_string)
        self.accreditations = Accreditations(root)

    def test_get_multiple_accreditations(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_with_accreditations.xml")
        )["KIS"]["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_accreditation_resp.json")
        )
        accreditations = get_accreditations(
            raw_course_xml, self.accreditations
        )

        self.assertListEqual(accreditations, expected_response)
