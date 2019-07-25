"""
This module extracts institution information from the HESA
XML dataset and writes it in JSON format to Cosmos DB.

Currently, we handle unexpected exceptions by letting
them bubble up. This should help flush out problems
during development and testing.
"""
import datetime
import inspect
import logging
import os
import sys
import xml.etree.ElementTree as ET
import json

import xmltodict

# TODO set PATH in Azure and remove this
CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from EtlPipelineBlobTrigger.course_docs import get_code_label_entry
from kisaims import KisAims
from locations import Locations
from SharedCode import utils
from ukrlp_enricher import UkRlpCourseEnricher
import course_lookup_tables as lookup

class InstitutionDocs:

    def __init__(self):
        self.ukrlp_lookups = utils.get_ukrlp_lookups()

    def get_course(self, raw_course_data):
        course = {}
        distance_learning = get_code_label_entry(raw_course_data,
                                                lookup.distance_learning_lookup,
                                                'distance')
        if distance_learning:
            course['distance_learning'] = distance_learning
        if 'honors' in raw_course_data:
            course['honours_award_provision'] = raw_course_data['honors']
        course['kis_course_id'] = raw_course_data['kiscourseid']
        mode = get_code_label_entry(raw_course_data, lookup.mode, 'kismode')
        if mode:
            course['mode'] = mode
        return course

    def get_contact_details(self, ukprn):
        if ukprn not in self.ukrlp_lookups:
            return {'No contact details availble':f'UKPRN: {ukprn}'}
        return self.ukrlp_lookups[ukprn]['contact_details']

    def get_ukprn_name(self, ukprn):
        if ukprn not in self.ukrlp_lookups:
            return {'No name availble':f'UKPRN: {ukprn}'}
        return self.ukrlp_lookups[ukprn]['']

    def get_institution_element(self, raw_inst_data):
        inst = {}
        if 'APROutcome' in raw_inst_data:
            inst['apr_outcome'] = raw_inst_data['APROutcome']
        inst['contact_details'] = self.get_contact_details(raw_inst_data['UKPRN'])
        inst['pub_ukprn_name'] = self.get_pub_ukprn_name(raw_inst_data['PUBUKPRN'])
        inst['pub_ukprn'] = raw_inst_data['PUBUKPRN'])
        return inst

    def get_institution_doc(self, institution):
        raw_inst_data = xmltodict.parse(
            ET.tostring(institution))['INSTITUTION']
        outer_wrapper = {}
        outer_wrapper['id'] = utils.get_uuid()
        outer_wrapper['created_at'] = datetime.datetime.utcnow().isoformat()
        outer_wrapper['version'] = 1
        outer_wrapper['institution_id'] = raw_inst_data['PUBUKPRN']

        outer_wrapper['institution'] = self.get_institution_element(raw_inst_data)
        return outer_wrapper


    def create_institution_docs(self, xml_string):
        """Parse HESA XML passed in and create JSON institution docs in Cosmos DB."""

        # TODO Investigate writing docs to CosmosDB in bulk to speed things up.
        logging.info(f"create_institution_docs entry \n")
        cosmosdb_client = utils.get_cosmos_client()

        enricher = UkRlpCourseEnricher()

        collection_link = utils.get_collection_link(
            'AzureCosmosDbDatabaseId', 'AzureCosmosDbInstitutionsCollectionId')

        # Import the XML dataset
        root = ET.fromstring(xml_string)

        # Import kisaims and location nodes
        kisaims = KisAims(root)
        locations = Locations(root)

        inst_count = 0
        for institution in root.iter('INSTITUTION'):
            inst_count += 1
            institution_doc = self.get_institution_doc(institution)
            cosmosdb_client.CreateItem(collection_link, institution_doc)
        logging.info(f"Processed {inst_count} institutions")
