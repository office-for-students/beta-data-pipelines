import inspect
import json
import os
import sys
import unittest
import xml.etree.ElementTree as ET

import xmltodict
from course_stats import Tariff


def get_string(filename):
    """Reads file in test dir into a string and returns"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as fp:
        string = fp.read()
    return string


class TestGetLookupTable(unittest.TestCase):
    def setUp(self):
        self.tariff = Tariff()

    def test_is_tariff(self):
        self.assertTrue(self.tariff.is_tariff('T001'))
        self.assertTrue(self.tariff.is_tariff('T048'))
        self.assertFalse(self.tariff.is_tariff('T999'))
        print(self.tariff.tariff_points_lookup.keys())



# TODO Test more of the functionality - more lookups etc

if __name__ == '__main__':
    unittest.main()
