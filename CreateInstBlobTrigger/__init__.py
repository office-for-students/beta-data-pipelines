#!/usr/bin/env python

""" EtlPipelineBlobTrigger: Build Institution Data based on a BLOB trigger """

import gzip
import io
import logging
import os
from datetime import datetime
from distutils.util import strtobool

import azure.functions as func
from azure.storage.blob import BlockBlobService

from . import institution_docs
from SharedCode import exceptions

def main(xmlblob: func.InputStream, context: func.Context):

    logging.info(f"CreateInstBlobTrigger Python BLOB trigger function processing BLOB \n"
                 f"Name: {xmlblob.name}\n"
                 f"Blob Size: {xmlblob.length} bytes")

    try:

        """ 0. PREPARATION """

        pipeline_start_datetime = datetime.today().strftime('%Y%m%d %H%M%S')

        xsd_filename = os.environ['XsdFilename']
        xsd_path = os.path.join(context.function_directory, xsd_filename)

        logging.info(f"CreateInstBlobTrigger configuration values \n"
                 f"XsdFilename: {xsd_filename}\n"
                 f"XsdPath: {xsd_path}")

        """ 1. DECOMPRESSION - Decompress the compressed HESA XML """
        # Note the HESA XML blob provided to this function will be gzip compressed.
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

        institution_docs.create_institution_docs(xml_string)

        """ 4. CLEANUP """

        pipeline_end_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.info('CreateInstBlobTrigger successfully finished on ' + pipeline_end_datetime)

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
