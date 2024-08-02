import unittest

import defusedxml.ElementTree as ET
import xmltodict

from legacy.CreateInst.institution_docs import add_tef_data
from legacy.CreateInst.tests.test_helpers.inst_test_utils import get_string


class TestTefData(unittest.TestCase):
    def setUp(self) -> None:
        self.xml_string = get_string("fixtures/inst_test.xml")
        self.xml_root = ET.fromstring(self.xml_string)
        self.total_institutions = 516

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
                self.assertEqual(tef_outcome["student_experience_rating"],
                                 raw_inst_data["TEFOutcome"]["STUDENT_EXPERIENCE_RATING"])
                self.assertEqual(tef_outcome["student_outcomes_rating"],
                                 raw_inst_data["TEFOutcome"]["STUDENT_OUTCOMES_RATING"])
                self.assertEqual(tef_outcome["outcome_url"], raw_inst_data["TEFOutcome"]["OUTCOME_URL"])
            else:
                self.assertIsNone(institution_element.get("tef_outcome"))

    # COMMENT THIS TEST OUT BEFORE RUNNING PIPELINE - requires testing of whole xml file
    # def test_total_tef(self):
    #     """test to check whether total number of institutions matches with xml - also writes output to json"""
    #     whole_xml = get_string("fixtures/latest.xml")
    #     xml_root = ET.fromstring(whole_xml)
    #     institution_docs = InstitutionDocs(whole_xml, version="test")
    #     inst_list = list()
    #     for institution in xml_root.iter("INSTITUTION"):
    #         inst_element = institution_docs.get_institution_element(institution)
    #         inst_list.append(inst_element)
    #         print(len(inst_list))
    #     with open("inst_with_tef_json_dump.json", 'w') as json_file:
    #         json.dump(inst_list, json_file, indent=4)
    #     self.assertEqual(len(inst_list), self.total_institutions)
