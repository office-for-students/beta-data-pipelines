import unittest
import defusedxml.ElementTree as ET

from CreateInst.institution_docs import add_tef_data
from CreateInst.tests.test_helpers.inst_test_utils import get_string
import xmltodict


class TestTefData(unittest.TestCase):
    def setUp(self) -> None:
        self.xml_string = get_string("fixtures/inst_test.xml")
        self.xml_root = ET.fromstring(self.xml_string)

    def test_tef_data_matches(self):
        for institution in self.xml_root.iter("INSTITUTION"):
            institution_element = dict()
            raw_inst_data = xmltodict.parse(ET.tostring(institution))[
                "INSTITUTION"
            ]
            if "TEFOutcome" in raw_inst_data:
                institution_element["tef_outcome"] = add_tef_data(raw_inst_data["TEFOutcome"])
                tef_outcome = institution_element["tef_outcome"]
                self.assertEqual(tef_outcome["report_ukprn"], raw_inst_data["TEFOutcome"]["REPORT_UKPRN"])
                self.assertEqual(tef_outcome["overall_rating"], raw_inst_data["TEFOutcome"]["OVERALL_RATING"])
                self.assertEqual(tef_outcome["student_experience_rating"], raw_inst_data["TEFOutcome"]["STUDENT_EXPERIENCE_RATING"])
                self.assertEqual(tef_outcome["student_outcomes_rating"], raw_inst_data["TEFOutcome"]["STUDENT_OUTCOMES_RATING"])
                self.assertEqual(tef_outcome["outcome_url"], raw_inst_data["TEFOutcome"]["OUTCOME_URL"])
            else:
                self.assertIsNone(institution_element.get("tef_outcome"))


