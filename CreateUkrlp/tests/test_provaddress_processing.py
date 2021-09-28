# address extracted from the test file provide by HESA on the 9th September
import os.path
import unittest

from CreateUkrlp import LookupCreator

TEST_FILE_ADDRESSES = "tests/fixtures/test_addresses.txt"
TEST_FILE_PHONE_NUMBER = "tests/fixtures/test_phone_numbers.txt"
TEST_FILE_WEBSITE = "tests/fixtures/test_website_urls.txt"

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(FILE_DIR)


def double_commas_in_string(address):
    return ",," in address


def space_before_comman_in_string(address):
    return " ," in address


class TestUKRLPAddressProcessor(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_address_to_dict(self):
        with open(os.path.join(PARENT_DIR, TEST_FILE_ADDRESSES)) as file:
            for line in file:
                address = LookupCreator.sanitise_address_string(line.rstrip())
                print(address)
                self.assertFalse(
                    double_commas_in_string(address=address),
                    f"Address: {address}, fails comma check"
                )
                self.assertFalse(
                    space_before_comman_in_string(address=address),
                    f"Address: {address} fails extra space check"
                )


class TestUKRLPTelephoneProcessor(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_address_to_dict(self):
        with open(os.path.join(PARENT_DIR, TEST_FILE_PHONE_NUMBER)) as file:
            for line in file:
                new_line = line.rstrip().replace(" ", "")
                self.assertTrue(new_line.isnumeric(), f"Expect only numbers if whitespace is removed: {new_line}")


class TestUKRLPWebsiteProcessor(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_address_to_dict(self):
        with open(os.path.join(PARENT_DIR, TEST_FILE_WEBSITE)) as file:
            for line in file:
                result = LookupCreator.normalise_url(line.rstrip())
                self.assertTrue(result[0] == "h")
                self.assertTrue(result[1] == "t")
                self.assertTrue(result[2] == "t")
                self.assertTrue(result[3] == "p")
                self.assertTrue(result[4] == "s")
                self.assertTrue(result[5] == ":")
                self.assertTrue(result[6] == "/")
                self.assertTrue(result[7] == "/")
