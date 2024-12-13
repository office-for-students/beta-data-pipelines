import inspect
import os
import sys
import unittest

from subject_build.validate import column_headers

CURRENT_DIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)


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
