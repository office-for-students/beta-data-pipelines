import inspect
import os
import unittest
import sys

CURRENTDIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0,PARENTDIR)

from course_stats import CourseStats

class TestGetContinuationKey(unittest.TestCase):

    def setUp(self):
        self.course_stats = CourseStats()

    def test_with_valid_key(self):
        expected_key = 'number_of_students'
        xml_key = 'CONTPOP'
        key = self.course_stats.get_continuation_key(xml_key)
        self.assertEqual(expected_key, key)


    def test_with_valid_key(self):
        invalid_xml_key = 'invalid_key'
        with self.assertRaises(KeyError):
            self.course_stats.get_continuation_key(invalid_xml_key)



if __name__ == '__main__':
    unittest.main()
