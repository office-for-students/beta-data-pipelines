import unittest

import defusedxml.ElementTree as ET

from legacy.CreateInst.institution_docs import get_country
from legacy.CreateInst.institution_docs import get_total_number_of_courses
from legacy.CreateInst.tests.test_helpers.inst_test_utils import get_first
from legacy.CreateInst.tests.test_helpers.inst_test_utils import get_string


class TestStaticHelperFunctions(unittest.TestCase):
    def test_get_total_number_of_courses_with_one_course(self):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        expected_number_of_courses = 1
        number_of_courses = get_total_number_of_courses(institution)
        self.assertEqual(expected_number_of_courses, number_of_courses)

    def test_get_total_number_of_courses_with_nine_courses(self):
        xml_string = get_string("fixtures/one_inst_nine_courses.xml")
        root = ET.fromstring(xml_string)
        institution = get_first(root.iter("INSTITUTION"))
        expected_number_of_courses = 9
        number_of_courses = get_total_number_of_courses(institution)
        self.assertEqual(expected_number_of_courses, number_of_courses)

    def get_country(self, expected_resp):
        code = expected_resp["code"]
        resp = get_country(code)
        return resp

    def test_get_country_england(self):
        expected_resp = {
            "code": "XF",
            "name": "England"
        }
        resp = self.get_country(expected_resp)
        self.assertEqual(expected_resp, resp)

    def test_get_country_wales(self):
        expected_resp = {
            "code": "XI",
            "name": "Wales"
        }
        resp = self.get_country(expected_resp)
        self.assertEqual(expected_resp, resp)

    def test_get_country_scotland(self):
        expected_resp = {
            "code": "XH",
            "name": "Scotland"
        }
        resp = self.get_country(expected_resp)
        self.assertEqual(expected_resp, resp)

    def test_get_country_ni(self):
        expected_resp = {
            "code": "XG",
            "name": "Northern Ireland"
        }
        resp = self.get_country(expected_resp)
        self.assertEqual(expected_resp, resp)


# class TestNewInstitutionData(unittest.TestCase):
#     def setUp(self) -> None:
#         self.one_inst_many_courses = get_string("fixtures/one_inst.xml")
#         self.root = ET.fromstring(self.one_inst_many_courses)
#
#     def test_get_total_number_of_courses(self):
#         institution = get_first(self.root.iter("INSTITUTION"))
#         expected_number_of_courses = 75
#         number_of_courses = get_total_number_of_courses(institution)
#         self.assertEqual(expected_number_of_courses, number_of_courses)


# TODO: Rework the test below
# class TestGetInstitutionDoc(unittest.TestCase):
#     def setUp(self):
#         kis_xml_string = get_string("fixtures/large_test_file.xml")
#         self.institution_docs = InstitutionDocs(kis_xml_string, 1)
#
#     # def test_with_large_file(self):
#     #     """Initial smoke test"""
#     #     xml_string = get_string("fixtures/large_test_file.xml")
#     #     root = ET.fromstring(xml_string)
#     #     for institution in root.iter("INSTITUTION"):
#     #         self.institution_docs.get_institution_doc(institution)
#
#     # def test_get_institution_doc(self):
#     #     xml_string = get_string("fixtures/one_inst.xml")
#     #     root = ET.fromstring(xml_string)
#     #     institution = get_first(root.iter("INSTITUTION"))
#     #     descendants = list(institution.iter())
#     #     expected_resp = json.loads(
#     #         get_string("fixtures/one_inst.xml")
#     #     )
#     #     # expected_resp = remove_variable_elements(expected_resp)
#     #     resp = self.institution_docs.get_institution_doc(descendants)
#     #     # resp = remove_variable_elements(resp)
#     #     self.assertEqual(expected_resp, resp)
#
#     @mock.patch.dict(os.environ, {"UseLocalTestXMLFile": "True", "LocalTestXMLFile": "fixtures/one_inst.xml"})
#     def test_get_inst_names(self):
#         xml_string = get_string("fixtures/one_inst.xml")
#         root = ET.fromstring(xml_string)
#         institution = get_first(root.iter("INSTITUTION"))
#         doc = InstitutionDocs(xml_string, 1)
#         resp = doc.get_institution_doc(institution)
#         print(resp["institution"], "RESP")
#         self.assertEqual(resp["institution"]["legal_name"], "Grwp Llandrillo Menai")
#         self.assertEqual(resp["institution"]["first_trading_name"], "Coleg Llandrillo")
#         self.assertEqual(resp["institution"]["other_names"], ["Coleg Menai", "Coleg Meirion-Dwyfor"])

# class TestCreateInstitutionDocs(unittest.TestCase):
#     @mock.patch.object(institution_docs.InstitutionDocs, "get_institution_doc")
#     @mock.patch("institution_docs.get_collection_link")
#     @mock.patch("institution_docs.get_cosmos_client")
#     # @mock.patch("institution_docs.get_ukrlp_lookups")
#     def test_create_institution_docs(
#             self,
#             # mock_get_ukrlp_lookups,
#             mock_get_cosmos_client,
#             mock_get_collection_link,
#             mock_get_institution_doc,
#     ):
#         kis_xml_string = get_string("fixtures/large_test_file.xml")
#         inst_docs = InstitutionDocs(kis_xml_string, 1)
#
#         inst_docs.create_institution_docs()
#         # mock_get_ukrlp_lookups.assert_called_once()
#         mock_get_cosmos_client.assert_called_once()
#         mock_get_collection_link.assert_called_once()
#         mock_get_institution_doc.assert_called()


if __name__ == "__main__":
    unittest.main()
