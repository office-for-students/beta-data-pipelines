"""

This module can be used during dev and debug for
testing the create_course_docs function without the need
to invoke it via the Azure function.
"""


#import logging
import inspect
import os
import sys

#logging.basicConfig(level=#logging.DEBUG)


CURRENTDIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
GRANDPARENTDIR = os.path.dirname(PARENTDIR)
GREATGRANDPARENTDIR = os.path.dirname(GRANDPARENTDIR)
sys.path.insert(0,CURRENTDIR)
sys.path.insert(0,PARENTDIR)
sys.path.insert(0,GRANDPARENTDIR)
sys.path.insert(0,GREATGRANDPARENTDIR)

from institution_docs import InstitutionDocs

def test_create_inst_docs():

    with open('institutions.xml', 'r') as file:
        xml_string = file.read()

    institution_docs = InstitutionDocs()
    institution_docs.create_institution_docs(xml_string)

test_create_inst_docs()
