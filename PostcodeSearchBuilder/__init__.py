"""Contains the entry point for Create Postcode Search
Azure Function implementation"""
import logging
import io
import gzip
import os
import traceback
from datetime import datetime

import azure.functions as func

from SharedCode.blob_helper import BlobHelper
from SharedCode.mail_helper import MailHelper

from . import search


def main(req: func.HttpRequest,) -> None:
    """Create the postcode search index"""

    logging.info(f"PostcodeSearchBuilder request triggered")

    mail_helper = MailHelper()
    environment = os.environ["Environment"]
    mail_helper.send_message(f"Postcode search builder started on {function_start_datetime}", f"Postcode Search Builder {environment} - {function_start_date} - Started")

    logging.info(
        f"PostcodeSearchBuilder function started on {function_start_datetime}"
    )

    api_key = os.environ['SearchAPIKey']
    search_url = os.environ['SearchURL']
    api_version = os.environ['AzureSearchAPIVersion']
    index_name = os.environ['PostcodeIndexName']

    try:
        blob_helper = BlobHelper()

        # Read the Blob into a BytesIO object
        storage_container_name = os.environ["AzureStoragePostcodesContainerName"]
        storage_blob_name = os.environ["AzureStoragePostcodesBlobName"]

        csv_string = blob_helper.get_str_file(storage_container_name, storage_blob_name)

        rows = csv_string.splitlines()
        number_of_postcodes = len(rows)

        # Create postcode search index
        search.build_index(search_url, api_key, api_version, index_name)

        # Add postcode documents to postcode search index
        logging.info(f'attempting to load postcodes to azure search\n\
                        number_of_postcodes: {number_of_postcodes}\n')
        search.load_index(search_url, api_key, api_version, index_name, rows)

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_end_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Postcode search builder completed on {function_end_datetime}", f"Postcode Search Builder {environment} - {function_end_date} - Completed")

        logging.info(
            f"PostcodeSearchBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Postcode search builder failed on {function_fail_datetime}", f"Postcode Search Builder {environment} - {function_fail_date} - Failed")

        logging.error(f"PostcodeSearchBuilder failed on {function_fail_datetime} ", exc_info=True)

        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
