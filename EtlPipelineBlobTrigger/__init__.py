#!/usr/bin/env python

""" EtlPipelineBlobTrigger: Execute the ETL pipeline based on a BLOB trigger """

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '../.env/lib/python3.6/site-packages')))

import azure.functions as func
import logging
import io
import gzip
from . import course_docs
from . import exceptions
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

        xsd_filename = os.environ['XsdFilename']
        xsd_path = os.path.join(context.function_directory, xsd_filename)

        logging.info(f"EtlPipelineBlobTrigger configuration values \n"
                 f"XsdFilename: {xsd_filename}\n"
                 f"XsdPath: {xsd_path}")

        """ 1. DECOMPRESSION - Decompress the compressed HESA XML """
        # Note the HESA XML blob provided will be gzip compressed.
        # This is a work around for a limitation discovered in Azure,
        # in that Functions written in Python do not get triggered 
        # correctly with large blobs. Tests showed this is not a limitation
        # with Funtions written in C#.

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the data 
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode('utf-8')


        """ 2. VALIDATION - Validate the HESA Raw XML against the XSD """
        
        # TODO fix failing validation. 
        # validators.validate_xml(xsd_path, xml_string)

        """ 3. LOADING - Parse XML and create enriched JSON Documents in Document Database """

        # For the purposes of the INITIAL end-to-end skeleton ETL pipeline only, 
        # this simply parses and loads INSTITUTION documents. This will need to be
        # updated with the full set of collection types and logic as per the
        # service data model and service query/access patterns.

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
