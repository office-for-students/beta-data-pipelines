import json
import unittest

import xmltodict
from course_docs import get_go_inst_json
from testing_utils import get_string


class TestGetGoInst(unittest.TestCase):

    def test_get_go_inst_no_data(self):
        element = "GOSALARY"

        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_go_inst_no_data.xml")
        )["KISCOURSE"][element]
        expected_response = json.loads(
            get_string("fixtures/course_go_inst_no_data_resp.json")
        )

        json_obj = get_go_inst_json(raw_course_xml)
        self.assertEqual(json_obj[0], expected_response[0])
