import json
import unittest

from lookup_creator import LookupCreator
from ukrlp_test_utils import get_string


class TestGetMatchingProviderRecords(unittest.TestCase):
    def test_get_website_100000794(self):
        matching_provider_records = json.loads(
            get_string("fixtures/ukrlp_10000794.json")
        )
        expected_website = "www.boltoncollege.ac.uk"
        website = LookupCreator.get_website(matching_provider_records)
        self.assertEqual(expected_website, website)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
