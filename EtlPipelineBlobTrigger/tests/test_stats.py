import unittest
import course_stats

class TestGetContinuationKey(unittest.TestCase):

    def test_with_valid_key(self):
        expected_key = 'number_of_students'
        xml_key = 'CONTPOP'
        key = course_stats.get_continuation_key(xml_key)
        print(key)
        self.assertEqual(expected_key, key)


if __name__ == '__main__':
    unittest.main()
