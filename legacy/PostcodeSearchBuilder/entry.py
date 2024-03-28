import logging
import traceback
from datetime import datetime
from typing import Any
from typing import Type

from constants import BLOB_POSTCODES_BLOB_NAME
from constants import BLOB_POSTCODES_CONTAINER_NAME
from constants import POSTCODE_INDEX_NAME
from constants import SEARCH_API_KEY
from constants import SEARCH_API_VERSION
from constants import SEARCH_URL
from legacy.PostcodeSearchBuilder.search import build_index
from legacy.PostcodeSearchBuilder.search import load_index


def postcode_search_builder_main(blob_service: type['BlobServiceBase']) -> dict[str, Any]:
    """Create the postcode search index"""
    response = {}

    logging.info(f"PostcodeSearchBuilder request triggered")

    # mail_helper = MailHelper()
    # mail_helper.send_message(f"Postcode search builder started on {function_start_datetime}", f"Postcode Search Builder {environment} - {function_start_date} - Started")

    # logging.info(
    #     f"PostcodeSearchBuilder function started on {function_start_datetime}"
    # )

    try:
        # Read the Blob into a BytesIO object
        csv_string = blob_service.get_str_file(
            container_name=BLOB_POSTCODES_CONTAINER_NAME,
            blob_name=BLOB_POSTCODES_BLOB_NAME
        )

        rows = csv_string.splitlines()
        number_of_postcodes = len(rows)

        # Create postcode search index
        build_index(
            url=SEARCH_URL,
            api_key=SEARCH_API_KEY,
            api_version=SEARCH_API_VERSION,
            index_name=POSTCODE_INDEX_NAME
        )

        # Add postcode documents to postcode search index
        logging.info(f'attempting to load postcodes to azure search\n\
                        number_of_postcodes: {number_of_postcodes}\n')
        load_index(
            url=SEARCH_URL,
            api_key=SEARCH_API_KEY,
            api_version=SEARCH_API_VERSION,
            index_name=POSTCODE_INDEX_NAME,
            rows=rows
        )

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_end_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(f"Postcode search builder completed on {function_end_datetime}", f"Postcode Search Builder {environment} - {function_end_date} - Completed")

        message = f"PostcodeSearchBuilder successfully finished on {function_end_datetime}"
        logging.info(message)

        response['message'] = message
        response['statusCode'] = 200

    except Exception as e:
        # Unexpected exception
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(f"Postcode search builder failed on {function_fail_datetime}", f"Postcode Search Builder {environment} - {function_fail_date} - Failed")

        message = f"PostcodeSearchBuilder failed on {function_fail_datetime} "
        logging.error(message, exc_info=True)
        logging.error(traceback.format_exc())

        # Raise to Azure
        # raise e

        response['message'] = message
        response['exception'] = traceback.format_exc()
        response['statusCode'] = 500

    return response
