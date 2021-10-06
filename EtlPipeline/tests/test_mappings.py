import unittest

from EtlPipeline.mappings.base import BaseMappings


class TestMappings(unittest.TestCase):
    def setUp(self):
        pass

    def test_none_or_na(self):
        key = "sould_be_na"
        data = dict()
        data[key] = "NA"
        data["lower_case"] = "na"
        data["exists"] = "I'm valid"
        self.assertFalse(BaseMappings.in_and_not_na(key=key, data=data))
        self.assertFalse(BaseMappings.in_and_not_na(key="Doesn't exist", data=data))
        self.assertFalse(BaseMappings.in_and_not_na(key="lower_case", data=data))
        self.assertTrue(BaseMappings.in_and_not_na(key="exists", data=data))
