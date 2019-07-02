"""Contains the entry point for Create UKRLP lookups Azure Function implementation"""

from datetime import datetime
import logging
import os
import io
import gzip
import traceback

import azure.functions as func

from .lookup_creator import LookupCreator

def main(xmlblob: func.InputStream):
    """Creates the UKRLP lookup tables for later use


    This Azure Function carries out the following steps:
    * Decompresses the XML HESA DataSet
    * Parses the INSTITUTION data from the DataSet
    * Retrieves enrichment data from the UKRLP API for each institution
    * Creates a lookup item for each Institution and writes it to CosmosDB

    """

    try:
        logging.info(f"CreateUkrlpBlobTrigger creating UKRLP lookups\n"
                     f"Name: {xmlblob.name}\n"
                     f"Blob Size: {xmlblob.length} bytes")

        pipeline_start_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.info('CreateUkrlp function started on ' + pipeline_start_datetime)

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the data
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode('utf-8')

        # TODO Validate the HESA XML

        # Parse the xml and create the lookups
        lookup_creator = LookupCreator(xml_string)
        lookup_creator.create_ukrlp_lookups()

        pipeline_end_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.info('CreateUkrlp successfully finished on ' + pipeline_end_datetime)

    except Exception as e:
        # Unexpected exception
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
