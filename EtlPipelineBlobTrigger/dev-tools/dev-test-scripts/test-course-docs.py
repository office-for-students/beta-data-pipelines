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

from course_docs import create_course_docs

def test_create_course_docs():
    """Function for testing LookupCreator"""

    with open('kis-test-file.xml', 'r') as file:
        xml_string = file.read()

    create_course_docs(xml_string)

test_create_course_docs()
