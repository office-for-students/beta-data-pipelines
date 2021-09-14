# address extracted from the test file provide by HESA on the 9th September
import os.path
import unittest

from CreateUkrlp import LookupCreator

TEST_FILE = "tests/fixtures/test_addresses.txt"
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
        with open(os.path.join(PARENT_DIR, TEST_FILE)) as file:
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
