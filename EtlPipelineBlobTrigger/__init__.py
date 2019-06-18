#!/usr/bin/env python

""" EtlPipelineBlobTrigger: Execute the ETL pipeline based on a BLOB trigger """

import azure.functions as func
import logging
import os
import io
import zlib
import gzip
from . import course_docs

from . import exceptions
from . import loaders
from . import validators
from datetime import datetime
from distutils.util import strtobool

__author__ = "Jillur Quddus, Nathan Shumoogum"
__credits__ = ["Jillur Quddus", "Nathan Shumoogum"]
__version__ = "0.1"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@methods.co.uk"
__status__ = "Development"


def main(xmlblob: func.InputStream, context: func.Context):

    """ Master ETL Pipeline - note that currently, the end-to-end ETL pipeline is 
    executed via this single Azure Function which calls other Python functions
    embedded within the same deployment codebase (see imports above). 
    TO DO: Investigate if/how this pipeline can be broken down into individual 
    Azure Functions chained/integrated and orchestrated using Azure Data Factory 
    and/or Function App. """

    logging.info(f"EtlPipelineBlobTrigger Python BLOB trigger function processing BLOB \n"
                 f"Name: {xmlblob.name}\n"
                 f"Blob Size: {xmlblob.length} bytes")

    try:

        """ 0. PREPARATION """

        # Log the start of the ETL Pipeline execution
        pipeline_start_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.info('ETL Pipeline started on ' + pipeline_start_datetime)

        # Get the relevant properties from Application Settings
        cosmosdb_uri = os.environ['AzureCosmosDbUri']
        cosmosdb_key = os.environ['AzureCosmosDbKey']
        cosmosdb_database_id = os.environ['AzureCosmosDbDatabaseId']
        cosmosdb_collection_id = os.environ['AzureCosmosDbCollectionId']
        xsd_filename = os.environ['XsdFilename']
        xsd_path = os.path.join(context.function_directory, xsd_filename)

        logging.info(f"EtlPipelineBlobTrigger configuration values \n"
                 f"XsdFilename: {xsd_filename}\n"
                 f"XsdPath: {xsd_path}")

        # Read the XML BLOB Input Stream
        # xml_string = xmlblob.read().decode('utf-8')
        # xml_string = xmlblob.read()

        # Note we are expecting the HESA dataset in Storage in gzip compressed
        # format. This is a work around because Functions written in Python
        # do not get triggered correctly with large blobs. This was not a problem when
        # tested with C#. 

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the file
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode('utf-8')


        """ 1. VALIDATION - Validate the HESA Raw XML against the XSD """
        
        # Is failing validation. Leave out for now.
        # validators.validate_xml(xsd_path, xml_string)

        """ 2. TRANSFORMATION - Parse, clean and enrich the XML into JSON Documents """

        # TO BE COMPLETED - currently, and for the purposes of the INITIAL end-to-end
        # skeleton ETL pipeline only, simple parsing and loading (with no cleansing) is 
        # processed together in the loading function below. This will require 
        # refactoring for the proper pipeline.

        """ 3. LOADING - Load the JSON Documents into the Document Database """

        # For the purposes of the INITIAL end-to-end skeleton ETL pipeline only, 
        # this simply parses and loads INSTITUTION documents. This will need to be
        # updated with the full set of collection types and logic as per the
        # service data model and service query/access patterns.

        #loaders.load_json_documents(
        #    cosmosdb_uri, cosmosdb_key, cosmosdb_database_id, cosmosdb_collection_id, 
        #    xml_string)

        course_docs.create_course_docs(xml_string)

        """ 4. CLEANUP """

        pipeline_end_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.info('ETL Pipeline successfully finished on ' + pipeline_end_datetime)

    except exceptions.StopEtlPipelineWarningException:

        # A WARNING is raised during the ETL Pipeline and StopEtlPipelineOnWarning=True
        # For example, the incoming raw XML is not valid against its XSD
        error_message = 'A WARNING has been encountered during the ETL Pipeline. ' \
            'The Pipeline will be stopped since StopEtlPipelineOnWarning has been ' \
            'set to TRUE in the Application Settings.'
        logging.error(error_message)
        pipeline_fail_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.error('ETL Pipeline failed on ' + pipeline_fail_datetime)
        raise Exception(error_message)
