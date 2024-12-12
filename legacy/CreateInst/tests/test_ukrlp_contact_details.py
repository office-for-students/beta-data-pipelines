import unittest

from legacy.CreateInst.docs.institution_docs import normalise_url
from legacy.CreateInst.docs.name_handler import InstitutionProviderNameHandler


class TestGetStudentUnions(unittest.TestCase):

    def test_normalised_url(self):
        test_urls = [
            "www.example.com",
            "http://example.com",
            "https://help.com",
            "example.com",
            "https://www.harperkeelevetschool.ac.uk/"
        ]
        empty = normalise_url("")
        self.assertEqual(empty, "")

        for line in test_urls:
            result = normalise_url(line.rstrip())
            print("result:", result)
            self.assertTrue(result[0] == "h")
            self.assertTrue(result[1] == "t")
            self.assertTrue(result[2] == "t")
            self.assertTrue(result[3] == "p")
            self.assertTrue(result[4] == "s")
            self.assertTrue(result[5] == ":")
            self.assertTrue(result[6] == "/")
            self.assertTrue(result[7] == "/")

    def test_provider_name_formatted(self):
        test_name = "an university and polYTech THAT for instance IN (and around) oF THE world"
        expected_result = "An University and Polytech That for Instance in (And Around) of the World"
        handle_apostrophe = "This of university's title (the)"
        handle_apostrophe_result = "This of University's Title (The)"
        handler = InstitutionProviderNameHandler(white_list=[], welsh_uni_names=[])
        result = handler.presentable(test_name)
        self.assertEqual(result, expected_result, f"{result} should match {expected_result}")

        result = handler.presentable(handle_apostrophe)
        self.assertEqual(result, handle_apostrophe_result, f"{result} should match {handle_apostrophe_result}")

    def test_provider_name_on_whitelist(self):
        test_name = "an university and polYTech THAT for instance IN (and around) oF THE world"
        white_list = [test_name]
        handler = InstitutionProviderNameHandler(white_list=white_list, welsh_uni_names=[])
        result = handler.presentable(test_name)
        self.assertEqual(result, test_name, f"{result} should match {test_name}")
