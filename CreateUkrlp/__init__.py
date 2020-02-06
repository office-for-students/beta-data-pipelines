"""Contains the entry point for Create UKRLP lookups Azure Function implementation"""

from datetime import datetime
import logging
import os
import io
import gzip
import traceback

import azure.functions as func

from azure.storage.blob import BlockBlobService

from SharedCode.dataset_helper import DataSetHelper
from SharedCode.blob_helper import BlobHelper
from SharedCode.mail_helper import MailHelper

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

    mail_helper = MailHelper()
    environment = os.environ["Environment"]

    try:
        logging.info(
            f"CreateUkrlp message queue triggered"
        )

        dsh = DataSetHelper()

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CreateUkrlp function started on {function_start_datetime}"
        )

        blob_helper = BlobHelper()

        storage_container_name = os.environ["AzureStorageHesaContainerName"]
        storage_blob_name = os.environ["AzureStorageHesaBlobName"]

        xml_string = blob_helper.get_str_file(storage_container_name, storage_blob_name)

        # Parse the xml and create the lookups
        version = dsh.get_latest_version_number()
        logging.info(f"using version number: {version}")
        dsh.update_status("institutions", "in progress")
        lookup_creator = LookupCreator(xml_string, version)
        lookup_creator.create_ukrlp_lookups()

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CreateUkrlp successfully finished on {function_end_datetime}"
        )

        msgout.set(f"CreateUkrlp successfully finished on {function_end_datetime}")

    except Exception as e:
        # Unexpected exception
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Automated data import failed on {function_fail_datetime} at CreateUkrlp", f"Data Import {environment} - {function_fail_date} - Failed")

        logging.error(f"CreateUkrlp faile on {function_fail_datetime}")
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
