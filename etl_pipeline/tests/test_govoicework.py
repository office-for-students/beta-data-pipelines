import json
import unittest
from unittest import mock
from unittest.mock import MagicMock

import xmltodict

from etl_pipeline.subject_enricher import SubjectCourseEnricher
from institution_creation.tests.test_helpers.inst_test_utils import get_string
from etl_pipeline.course_docs import get_go_voice_work_json


class TestGetGoVoiceWork(unittest.TestCase):
    def test_get_govoicework_no_data(self):
        element = "GOVOICEWORK"

        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_govoicework_no_data.xml")
        )["KISCOURSE"][element]
        expected_response = json.loads(
            get_string("fixtures/course_govoicework_no_data_resp.json")
        )
        subject_enricher = MagicMock(SubjectCourseEnricher)
        json_obj = get_go_voice_work_json(raw_course_xml, subject_enricher, subject_codes={})
        self.assertEqual(json_obj[0], expected_response[0])
