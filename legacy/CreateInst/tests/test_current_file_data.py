# address extracted from the test file provide by HESA on the 9th September
import os.path
import unittest

from legacy.CreateInst.docs.institution_docs import sanitise_address_string

TEST_FILE_ADDRESSES = "tests/fixtures/hesa_test_data/test_addresses.txt"
TEST_FILE_PHONE_NUMBER = "tests/fixtures/hesa_test_data/test_phone_numbers.txt"
TEST_FILE_WEBSITE = "tests/fixtures/hesa_test_data/test_website_urls.txt"

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
                address = sanitise_address_string(line.rstrip())
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

    def test_phone_numbers_are_just_numbers(self):
        with open(os.path.join(PARENT_DIR, TEST_FILE_PHONE_NUMBER)) as file:
            for line in file:
                new_line = line.rstrip().replace(" ", "")
                self.assertTrue(new_line.isnumeric(), f"Expect only numbers if whitespace is removed: {new_line}")
