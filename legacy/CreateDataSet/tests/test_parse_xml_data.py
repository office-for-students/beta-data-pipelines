import unittest

from legacy.CreateDataSet.tests.test_helpers.dataset_test_utils import get_string
from legacy.CreateDataSet.validators import parse_xml_data
from legacy.services.exceptions import XmlValidationError


class TestParseXmlData(unittest.TestCase):
    def test_parse_xml_data(self):
        valid_xml = get_string("fixtures/large_test_file.xml")
        try:
            parse_xml_data(valid_xml)
        except XmlValidationError:
            self.fail("validate_feedback raised unexpected ValidationError")


# TODO add more tests
