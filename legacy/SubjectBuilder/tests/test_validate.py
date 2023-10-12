import unittest
import os
import sys
import inspect

CURRENTDIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from validate import column_headers


class TestValidateColumnHeaders(unittest.TestCase):
    def test_when_headers_are_valid(self):
        header_row = "code,english_label,level,welsh_label"

        output_result = column_headers(header_row)
        self.assertEqual(True, output_result)

    def test_when_headers_are_in_wrong_order(self):
        header_row = "code,english_label,welsh_label,level"

        output_result = column_headers(header_row)
        self.assertEqual(False, output_result)

    def test_when_headers_are_incorrect(self):
        header_row = "code,english,level,welsh"

        output_result = column_headers(header_row)
        self.assertEqual(False, output_result)

    def test_when_there_are_not_enough_headers(self):
        header_row = "code,english_label,level"

        output_result = column_headers(header_row)
        self.assertEqual(False, output_result)