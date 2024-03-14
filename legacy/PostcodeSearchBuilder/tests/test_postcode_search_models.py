import inspect
import os
import sys
import unittest

from legacy.PostcodeSearchBuilder.models import build_postcode_search_doc
from legacy.PostcodeSearchBuilder.models import validate_header
from legacy.PostcodeSearchBuilder.models import validate_latitude
from legacy.PostcodeSearchBuilder.models import validate_longitude

CURRENT_DIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)


class TestValidateLatitude(unittest.TestCase):
    def test_within_latitudinal_range(self):
        input_latitude = 50.456382
        expected_result = False

        result = validate_latitude(input_latitude)
        self.assertEqual(expected_result, result)

    def test_outside_latitudinal_minimum(self):
        input_latitude = 48.456382
        expected_result = True

        result = validate_latitude(input_latitude)
        self.assertEqual(expected_result, result)

    def test_outside_latitudinal_maximum(self):
        input_latitude = 61.456382
        expected_result = True

        result = validate_latitude(input_latitude)
        self.assertEqual(expected_result, result)


class TestValidateLongitude(unittest.TestCase):
    def test_within_longitudinal_range(self):
        input_longitude = -4.5738109
        expected_result = False

        result = validate_longitude(input_longitude)
        self.assertEqual(expected_result, result)

    def test_outside_longitudinal_minimum(self):
        input_longitude = -11.5738109
        expected_result = True

        result = validate_longitude(input_longitude)
        self.assertEqual(expected_result, result)

    def test_outside_longitudinal_maximum(self):
        input_longitude = 3.456382
        expected_result = True

        result = validate_longitude(input_longitude)
        self.assertEqual(expected_result, result)


class TestValidateHeader(unittest.TestCase):
    def test_header_row_is_correct(self):
        valid_header_row = ["id", "postcode", "latitude", "longitude"]
        expected_result = False

        result = validate_header(valid_header_row)
        self.assertEqual(expected_result, result)

    def test_with_incorrect_item_1(self):
        invalid_header_row = [
            "identifier",
            "postcode",
            "latitude",
            "longitude"
        ]
        expected_result = True

        result = validate_header(invalid_header_row)
        self.assertEqual(expected_result, result)

    def test_with_incorrect_item_2(self):
        invalid_header_row = ["id", "postal_code", "latitude", "longitude"]
        expected_result = True

        result = validate_header(invalid_header_row)
        self.assertEqual(expected_result, result)

    def test_with_incorrect_item_3(self):
        invalid_header_row = ["id", "postcode", "lat", "longitude"]
        expected_result = True

        result = validate_header(invalid_header_row)
        self.assertEqual(expected_result, result)

    def test_with_incorrect_item_4(self):
        invalid_header_row = ["id", "postcode", "latitude", "long"]
        expected_result = True

        result = validate_header(invalid_header_row)
        self.assertEqual(expected_result, result)


class TestBuildPostcodeSearchDoc(unittest.TestCase):
    def test_successful_postcode_search_doc(self):
        postcode_list = ["1", "CF5 1AB", "51.4860", "-3.2282"]
        expected_result = {
            "@search.action": "upload",
            "id": "1",
            "geo": {
                "type": "Point",
                "coordinates": [-3.2282, 51.486]
            },
            "latitude": 51.486,
            "longitude": -3.2282,
            "postcode": "cf51ab"
        }

        result = build_postcode_search_doc(postcode_list)
        self.assertEqual(expected_result, result)

    def test_unsuccessful_postcode_search_doc(self):
        postcode_list = ["2", "10014", "-74.0094", "40.7366"]
        expected_result = None

        result = build_postcode_search_doc(postcode_list)
        self.assertEqual(expected_result, result)

    def test_postcode_search_doc_not_a_float(self):
        postcode_list = ["3", "10014", "-74.0094", "forty"]
        expected_result = None

        result = build_postcode_search_doc(postcode_list)
        self.assertEqual(expected_result, result)
