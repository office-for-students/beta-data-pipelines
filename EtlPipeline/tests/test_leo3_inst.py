import json
import unittest

import xmltodict

from EtlPipeline.course_docs import get_leo3_inst_json
from EtlPipeline.tests.test_helpers.testing_utils import get_string


class TestGetLeo3Inst(unittest.TestCase):

    def test_get_leo3_inst_no_data(self):
        element = "LEO3"

        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_leo3_inst_no_data.xml")
        )["KISCOURSE"][element]
        expected_response = json.loads(
            get_string("fixtures/course_leo3_inst_no_data_resp.json")
        )

        json_obj = get_leo3_inst_json(raw_course_xml)
        self.assertEqual(json_obj[0], expected_response[0])
