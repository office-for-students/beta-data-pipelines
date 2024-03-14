import json
import unittest
from unittest import mock

import xmltodict

from legacy.CreateInst.tests.test_helpers.inst_test_utils import get_string
from legacy.EtlPipeline.course_docs import get_go_voice_work_json


class TestGetGoVoiceWork(unittest.TestCase):
    @mock.patch("EtlPipeline.subject_enricher.SubjectCourseEnricher")
    def test_get_govoicework_no_data(self, subject_enricher):
        element = "GOVOICEWORK"

        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_govoicework_no_data.xml")
        )["KISCOURSE"][element]
        expected_response = json.loads(
            get_string("fixtures/course_govoicework_no_data_resp.json")
        )

        json_obj = get_go_voice_work_json(raw_course_xml, subject_enricher)
        self.assertEqual(json_obj[0], expected_response[0])
