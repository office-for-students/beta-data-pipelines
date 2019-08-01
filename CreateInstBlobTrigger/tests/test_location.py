import json
import unittest
import xml.etree.ElementTree as ET

from unittest import mock

from inst_test_utils import get_first, get_string, remove_variable_elements
from CreateInstBlobTrigger.locations import Locations
import institution_docs
from institution_docs import (
    InstitutionDocs,
    get_country,
    get_student_unions,
    get_total_number_of_courses,
)

class TestLocationLookup(unittest.TestCase):

    def test_get_location_for_key(self):
        kis_xml_string = get_string("fixtures/large_test_file.xml")
        kis_root = ET.fromstring(kis_xml_string)
        locations = Locations(kis_root)
        institution_pubukprn = '10000047'
        locid = 'A1'
        lookup_key = f'{institution_pubukprn}{locid}'
        location_data = locations.get_location_data_for_key(lookup_key)
        self.assertEqual(location_data['UKPRN'], '10001143')
        self.assertEqual(location_data['LOCID'], 'A1')
        self.assertEqual(location_data['LOCUKPRN'], '10000047')
        self.assertEqual(location_data['LOCCOUNTRY'], 'XF')
        self.assertEqual(location_data['SUURL'], 'http://ccsu.co.uk/')

# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
