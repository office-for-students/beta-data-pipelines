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
    def test_title_case_needed_with_lipa(
        self, mock_get_cosmos_client, mock_get_collection_link
    ):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        lookup_creator = LookupCreator(xml_string)
        self.assertFalse(lookup_creator.title_case_needed("LIPA"))

    @mock.patch("lookup_creator.get_collection_link")
    @mock.patch("lookup_creator.get_cosmos_client")
    def test_title_case_needed_with_bexhill_college(
        self, mock_get_cosmos_client, mock_get_collection_link
    ):
        xml_string = get_string("fixtures/one_inst_one_course.xml")
        lookup_creator = LookupCreator(xml_string)
        self.assertTrue(lookup_creator.title_case_needed("BEXHILL COLLEGE"))


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
