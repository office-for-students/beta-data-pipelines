import json
import unittest

from unittest import mock
from lookup_creator import LookupCreator
from ukrlp_test_utils import get_string


class TestGetWebSite(unittest.TestCase):
    def test_get_website_100000794(self):
        matching_provider_records = json.loads(
            get_string("fixtures/ukrlp_10000794.json")
        )
        expected_website = "www.boltoncollege.ac.uk"
        website = LookupCreator.get_website(matching_provider_records)
        self.assertEqual(expected_website, website)


class TestNeedTitleCase(unittest.TestCase):
    @mock.patch("lookup_creator.get_collection_link")
    @mock.patch("lookup_creator.get_cosmos_client")
    @mock.patch("__app__.SharedCode.dataset_helper.get_collection_link")
    @mock.patch("__app__.SharedCode.dataset_helper.get_cosmos_client")
    def test_title_case_needed_with_lipa(
        self, mock_get_cosmos_client, mock_get_collection_link, mock_dsh__get_collection_link, mock_dsh_get_cosmos_client
    ):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        lookup_creator = LookupCreator(xml_string, 1)
        self.assertFalse(lookup_creator.title_case_needed("LIPA"))

    @mock.patch("lookup_creator.get_collection_link")
    @mock.patch("lookup_creator.get_cosmos_client")
    @mock.patch("__app__.SharedCode.dataset_helper.get_collection_link")
    @mock.patch("__app__.SharedCode.dataset_helper.get_cosmos_client")
    def test_title_case_needed_with_bexhill_college(
        self, mock_get_cosmos_client, mock_get_collection_link, mock_dsh__get_collection_link, mock_dsh_get_cosmos_client
    ):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        lookup_creator = LookupCreator(xml_string, 1)
        self.assertTrue(lookup_creator.title_case_needed("BEXHILL COLLEGE"))


class TestTitleCase(unittest.TestCase):
    def test_title_case_with_bangor_university(self):
        input_name = "BANGOR UNIVERSITY"
        expected_name = "Bangor University"
        self.assertTrue(LookupCreator.title_case(input_name), expected_name)

    def test_title_case_with_blackpool_and_flyde_college(self):
        input_name = "BLACKPOOL AND THE FYLDE COLLEGE"
        expected_name = "Blackpool and the Fylde College"
        self.assertTrue(LookupCreator.title_case(input_name), expected_name)

    def test_title_case_with_british_college_of_osteopathic_medicine(self):
        input_name = "British College Of Osteopathic Medicine"
        expected_name = "British College of Osteopathic Medicine"
        self.assertTrue(LookupCreator.title_case(input_name), expected_name)


class TestGetContactDetails(unittest.TestCase):
    def test_get_contact_details_for_10000794(self):
        expected_contact_details = {
            "address": {
                "line_2": "Deane Road Campus",
                "line_3": "Deane Road",
                "town": "Bolton",
                "post_code": "BL3 5BG",
            },
            "telephone": "01204 482000",
            "website": "www.boltoncollege.ac.uk",
        }

        matching_provider_records = json.loads(
            get_string("fixtures/ukrlp_10000794.json")
        )
        contact_details = LookupCreator.get_contact_details(
            "10000794", matching_provider_records
        )
        self.assertDictEqual(contact_details, expected_contact_details)

    def test_get_contact_details_for_10004079(self):
        expected_contact_details = {
            "address": {
                "line_2": "5",
                "line_3": "Nether Street",
                "town": "London",
                "post_code": "N12 0GA",
            },
            "telephone": "020 7837 7741",
            "website": "www.londonstudiocentre.org",
        }

        matching_provider_records = json.loads(
            get_string("fixtures/ukrlp_10004079.json")
        )
        contact_details = LookupCreator.get_contact_details(
            "10004079", matching_provider_records
        )
        self.assertDictEqual(contact_details, expected_contact_details)

    def test_get_contact_details_for_10001282(self):
        expected_contact_details = {
            "address": {
                "line_1": "Ellison Building",
                "line_2": "Ellison Place",
                "town": "Newcastle Upon Tyne",
                "post_code": "NE1 8ST",
            },
            "telephone": "01912 326002",
            "website": "www.northumbria.ac.uk",
        }

        matching_provider_records = json.loads(
            get_string("fixtures/ukrlp_10001282.json")
        )
        contact_details = LookupCreator.get_contact_details(
            "10001282", matching_provider_records
        )
        self.assertDictEqual(contact_details, expected_contact_details)

    def test_get_contact_details_for_10013109(self):
        expected_contact_details = {
            "address": {
                "line_1": "53 Farringdon Road",
                "town": "London",
                "post_code": "EC1M 3JB",
            },
            "telephone": "020 7378 1000",
            "website": "No website available",
        }

        matching_provider_records = json.loads(
            get_string("fixtures/ukrlp_10013109.json")
        )
        contact_details = LookupCreator.get_contact_details(
            "10001282", matching_provider_records
        )
        self.assertDictEqual(contact_details, expected_contact_details)


if __name__ == "__main__":
    unittest.main()
