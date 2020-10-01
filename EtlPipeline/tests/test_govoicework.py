import json
import unittest

import xmltodict
from testing_utils import get_string
from course_docs import get_go_voice_work_json


class TestGetGoVoiceWork(unittest.TestCase):

    def test_get_govoicework_no_data(self):
        element = "GOVOICEWORK"

        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_govoicework_no_data.xml")
        )["KISCOURSE"][element]
        expected_response = json.loads(
            get_string("fixtures/course_govoicework_no_data_resp.json")
        )

        json_obj = get_go_voice_work_json(raw_course_xml)
        self.assertEqual(json_obj[0], expected_response[0])
