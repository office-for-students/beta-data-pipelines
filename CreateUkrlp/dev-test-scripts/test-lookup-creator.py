"""

This module can be used during dev and debug for
testing the LookupCreator class without the need
to invoke the Azure function.
"""


import logging
import inspect
import os
import sys

logging.basicConfig(level=logging.DEBUG)


CURRENTDIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
GRANDPARENTDIR = os.path.dirname(PARENTDIR)
sys.path.insert(0,PARENTDIR)
sys.path.insert(0,GRANDPARENTDIR)

from lookup_creator import LookupCreator

def test_lookup_creator():
    """Function for testing LookupCreator"""

    # Change the open line as required during test/debug
    with open('../../../kis.xml', 'r') as file:
    #with open('../../test-data/institutions.xml', 'r') as file:
        xml_string = file.read()

    lookup_creator = LookupCreator(xml_string, "", 1)
    lookup_creator.create_ukrlp_lookups()


test_lookup_creator()
