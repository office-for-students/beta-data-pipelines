"""
This module can be used during dev and debug for
testing the create_course_docs function without the need
to invoke it via the Azure function.
"""

import inspect
import logging
import os
import sys

from legacy.CreateInst.docs.institution_docs import InstitutionDocs

logging.basicConfig(level=logging.DEBUG)

CURRENT_DIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
GRANDPARENT_DIR = os.path.dirname(PARENT_DIR)
GREAT_GRANDPARENT_DIR = os.path.dirname(GRANDPARENT_DIR)
sys.path.insert(0, CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)
sys.path.insert(0, GRANDPARENT_DIR)
sys.path.insert(0, GREAT_GRANDPARENT_DIR)


def test_create_inst_docs():
    with open('institutions.xml', 'r') as file:
        xml_string = file.read()

    institution_docs = InstitutionDocs(xml_string, 1)
    institution_docs.create_institution_docs()


test_create_inst_docs()
