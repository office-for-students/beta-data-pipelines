import datetime
import json
import logging
import os
import pprint
import sys
import traceback
import uuid
import xml.etree.ElementTree as ET
import xmltodict

import azure.cosmos.errors as errors

import  lookup_creator

def test_lookup_creator():

    with open('path to your test file here', 'r') as file:
        xml_string = file.read()

    lc = lookup_creator.LookupCreator(xml_string)
    lc.create_ukrlp_lookups()


test_lookup_creator()
