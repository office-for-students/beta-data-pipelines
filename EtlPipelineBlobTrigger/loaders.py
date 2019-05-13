#!/usr/bin/env python

""" transformers.py: ETL Pipeline Parsers and Transformers """

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.documents as documents
import azure.cosmos.errors as errors
import azure.functions as func
import json
import logging
import os
import xml.etree.cElementTree as ET

from xmljson import yahoo

__author__ = "Jillur Quddus, Nathan Shumoogum"
__credits__ = ["Jillur Quddus", "Nathan Shumoogum"]
__version__ = "0.1"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@methods.co.uk"
__status__ = "Development"

# Load the relevant XPaths from Application Settings
xpath_institution = os.environ['XPathInstitution']


def load_json_documents(
    cosmosdb_uri, cosmosdb_key, cosmosdb_database_id, cosmosdb_collection_id, 
    xml_file_or_string):

    """ For the purposes of the INITIAL end-to-end skeleton ETL pipeline ONLY and
    demonstrating how to load JSON documents into the CosmosDB Document Database, 
    this simply parses the raw HESA XML, and generates and loads INSTITUTION JSOON 
    documents. This will need to be updated with the full set of collection types 
    and logic as per the data model and required query/access patterns. """ 

    # Create a CosmosDB Client
    cosmosdb_client = cosmos_client.CosmosClient(
        url_connection=cosmosdb_uri, auth={'masterKey': cosmosdb_key})
    logging.debug("Created a CosmosDB client connection to %s", cosmosdb_uri)

    # Define a link to the relevant CosmosDB Container/Document Collection
    collection_link = 'dbs/' + cosmosdb_database_id + '/colls/' + cosmosdb_collection_id

    # Generate an Element Tree from File
    # tree = ET.parse(xml_file_or_string)
    # root = tree.getroot()

    # Generate an Element Tree from String
    root = ET.fromstring(xml_file_or_string)

    # Iterate over all Institutions
    document_counter = 0
    for institution in root.findall(xpath_institution):

        # Generate a JSON Document
        # To remove the 'INSTITUTION' key from the JSON document (i.e. move all keys
        # up one level), get the OrderedDict VALUES
        # json_document = json.dumps(yahoo.data(institution)) 
        json_document = json.dumps(list(yahoo.data(institution).values())[0])

        # Load the JSON Document into the CosmosDB Database
        # TO DO - Investigate more efficient bulk/batch load, and refactor accordingly
        cosmosdb_client.CreateItem(collection_link, json.loads(json_document))
        document_counter += 1

    logging.info("Loaded %d documents into collection '%s'", 
        document_counter, cosmosdb_collection_id)
    
