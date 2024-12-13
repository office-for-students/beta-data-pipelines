"""

This module can be used during dev and debug for
testing the create_course_docs function without the need
to invoke it via the Azure function.
"""


import logging
import inspect
import os
import sys

logging.basicConfig(level=logging.DEBUG)


CURRENT_DIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
GRANDPARENT_DIR = os.path.dirname(PARENT_DIR)
GREAT_GRANDPARENT_DIR = os.path.dirname(GRANDPARENT_DIR)
sys.path.insert(0, PARENT_DIR)
sys.path.insert(0, GRANDPARENT_DIR)
sys.path.insert(0, GREAT_GRANDPARENT_DIR)

# from legacy.etl_pipeline.course_docs import create_course_docs
#
#
# def test_create_course_docs():
#
#     with open("kis-test-file.xml", "r") as file:
#         xml_string = file.read()
#
#     create_course_docs(xml_string)
#
#
# test_create_course_docs()
