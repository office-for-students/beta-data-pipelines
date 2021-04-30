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


CURRENTDIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
GRANDPARENTDIR = os.path.dirname(PARENTDIR)
GREATGRANDPARENTDIR = os.path.dirname(GRANDPARENTDIR)
sys.path.insert(0, PARENTDIR)
sys.path.insert(0, GRANDPARENTDIR)
sys.path.insert(0, GREATGRANDPARENTDIR)

from course_docs import load_course_docs


def test_missing_sbj():

    with open("EtlPipeline/tests/fixtures/course_missing_sbj.xml", "r") as file:
        xml_string = file.read()

    load_course_docs(xml_string, 1)


test_missing_sbj()
