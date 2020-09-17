import json
import unittest

import xmltodict
from course_docs import get_leo5_inst_json
from testing_utils import get_string


class TestGetLeo5Inst(unittest.TestCase):

    def test_get_leo5_inst_no_data(self):
        element = "LEO5"

        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_leo5_inst_no_data.xml")
        )["KISCOURSE"][element]
        expected_response = json.loads(
            get_string("fixtures/course_leo5_inst_no_data_resp.json")
        )

        json_obj = get_leo5_inst_json(raw_course_xml)
        self.assertEqual(json_obj[0], expected_response[0])
