import logging
import traceback
from datetime import datetime

from decouple import config

from legacy.PostcodeSearchBuilder.search import build_index
from legacy.PostcodeSearchBuilder.search import load_index
from legacy.services.blob import BlobService


def postcode_search_builder_main() -> None:
    """Create the postcode search index"""

    logging.info(f"PostcodeSearchBuilder request triggered")

    # mail_helper = MailHelper()
    environment = config("ENVIRONMENT")
    # mail_helper.send_message(f"Postcode search builder started on {function_start_datetime}", f"Postcode Search Builder {environment} - {function_start_date} - Started")

    # logging.info(
    #     f"PostcodeSearchBuilder function started on {function_start_datetime}"
    # )

    api_key = config("SEARCH_API_KEY")
    search_url = config("SEARCH_URL")
    api_version = config("SEARCH_API_VERSION")
    index_name = config("POSTCODE_INDEX_NAME")

    try:
        blob_service = BlobService()

        # Read the Blob into a BytesIO object
        storage_container_name = config("BLOB_POSTCODES_CONTAINER_NAME")
        storage_blob_name = config("BLOB_POSTCODES_BLOB_NAME")

        csv_string = blob_service.get_str_file(storage_container_name, storage_blob_name)

        rows = csv_string.splitlines()
        number_of_postcodes = len(rows)

        # Create postcode search index
        build_index(search_url, api_key, api_version, index_name)

        # Add postcode documents to postcode search index
        logging.info(f'attempting to load postcodes to azure search\n\
                        number_of_postcodes: {number_of_postcodes}\n')
        load_index(search_url, api_key, api_version, index_name, rows)

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
