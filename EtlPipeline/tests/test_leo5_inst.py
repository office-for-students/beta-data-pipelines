import json
import unittest
from unittest import mock

import xmltodict

from EtlPipeline.course_docs import get_leo5_inst_json
from EtlPipeline.tests.test_helpers.testing_utils import get_string


class TestGetLeo5Inst(unittest.TestCase):
    @mock.patch("EtlPipeline.subject_enricher.SubjectCourseEnricher")
    def test_get_leo5_inst_no_data(self, subject_enricher):
        element = "LEO5"

        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_leo5_inst_no_data.xml")
        )["KISCOURSE"][element]
        expected_response = json.loads(
            get_string("fixtures/course_leo5_inst_no_data_resp.json")
        )

        json_obj = get_leo5_inst_json(raw_course_xml, subject_enricher)
        self.assertEqual(json_obj[0], expected_response[0])
