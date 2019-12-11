"""Contains the entry point for Create UKRLP lookups Azure Function implementation"""

from datetime import datetime
import logging
import os
import io
import gzip
import traceback

import azure.functions as func

from azure.storage.blob import BlockBlobService

from SharedCode.blob_helper import BlobHelper

from .lookup_creator import LookupCreator


def main(msgin: func.QueueMessage, msgout: func.Out[str]):
    """Creates the UKRLP lookup tables for later use

    This Azure Function carries out the following steps:
    * Decompresses the XML HESA DataSet

    * Parses the INSTITUTION data from the DataSet

    * Retrieves enrichment data from the UKRLP API for each institution

    * Creates a lookup item for each Institution and writes it to CosmosDB

    * Currently, once completed successfully this function triggers the Etl function by copying
      the compressed XML passed in to a Blob storage monitored by the Etl function.

    """

    try:
        logging.info(
            f"CreateUkrlp message queue triggered"
        )

        function_start_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CreateUkrlp function started on {function_start_datetime}"
        )

        blob_helper = BlobHelper()
        
        xml_string = blob_helper.get_hesa_xml()

        # Parse the xml and create the lookups
        lookup_creator = LookupCreator(xml_string)
        lookup_creator.create_ukrlp_lookups()

        function_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CreateUkrlp successfully finished on {function_end_datetime}"
        )

        msgout.set(f"CreateUkrlp successfully finished on {function_end_datetime}")

    except Exception as e:
        # Unexpected exception
        logging.error("Unexpected exception")
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
