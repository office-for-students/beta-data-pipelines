import logging
import traceback
from datetime import datetime

from constants import BLOB_POSTCODES_BLOB_NAME
from constants import BLOB_POSTCODES_CONTAINER_NAME
from constants import POSTCODE_INDEX_NAME
from constants import SEARCH_API_KEY
from constants import SEARCH_API_VERSION
from constants import SEARCH_URL
from legacy.PostcodeSearchBuilder.search import build_index
from legacy.PostcodeSearchBuilder.search import load_index
from legacy.services.blob import BlobService


def postcode_search_builder_main(blob_service: BlobService) -> None:
    """Create the postcode search index"""

    logging.info(f"PostcodeSearchBuilder request triggered")

    # mail_helper = MailHelper()
    # mail_helper.send_message(f"Postcode search builder started on {function_start_datetime}", f"Postcode Search Builder {environment} - {function_start_date} - Started")

    # logging.info(
    #     f"PostcodeSearchBuilder function started on {function_start_datetime}"
    # )

    try:
        # Read the Blob into a BytesIO object
        csv_string = blob_service.get_str_file(BLOB_POSTCODES_CONTAINER_NAME, BLOB_POSTCODES_BLOB_NAME)

        rows = csv_string.splitlines()
        number_of_postcodes = len(rows)

        # Create postcode search index
        build_index(SEARCH_URL, SEARCH_API_KEY, SEARCH_API_VERSION, POSTCODE_INDEX_NAME)

        # Add postcode documents to postcode search index
        logging.info(f'attempting to load postcodes to azure search\n\
                        number_of_postcodes: {number_of_postcodes}\n')
        load_index(SEARCH_URL, SEARCH_API_KEY, SEARCH_API_VERSION, POSTCODE_INDEX_NAME, rows)

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_end_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(f"Postcode search builder completed on {function_end_datetime}", f"Postcode Search Builder {environment} - {function_end_date} - Completed")

        logging.info(
            f"PostcodeSearchBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(f"Postcode search builder failed on {function_fail_datetime}", f"Postcode Search Builder {environment} - {function_fail_date} - Failed")

        logging.error(f"PostcodeSearchBuilder failed on {function_fail_datetime} ", exc_info=True)

        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
