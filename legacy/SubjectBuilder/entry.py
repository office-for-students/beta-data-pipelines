import csv
import logging
from datetime import datetime

from constants import BLOB_SUBJECTS_BLOB_NAME
from constants import BLOB_SUBJECTS_CONTAINER_NAME
from constants import COSMOS_COLLECTION_SUBJECTS
from constants import COSMOS_DATABASE_ID
from constants import COSMOS_DATABASE_KEY
from constants import COSMOS_DATABASE_URI
from legacy.SubjectBuilder.database import load_collection
from legacy.SubjectBuilder.validate import column_headers
from legacy.services import exceptions
from legacy.services.blob import BlobService
from legacy.services.dataset_service import DataSetService


def subject_builder_main() -> None:
    msgerror = ""

    dataset_service = DataSetService()

    logging.info(f"SubjectBuilder message queue triggered")

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

    # mail_helper = MailHelper()

    logging.info(
        f"SubjectBuilder function started on {function_start_datetime}"
    )

    try:
        blob_helper = BlobService()

        # Read the Blob into a BytesIO object
        csv_string = blob_helper.get_str_file(BLOB_SUBJECTS_CONTAINER_NAME, BLOB_SUBJECTS_BLOB_NAME)

        rows = csv_string.splitlines()
        number_of_subjects = len(rows) - 1

        # csv header row
        if not column_headers(rows[0]):
            logging.error(
                "file headers are incorrect, expecting the following: code, english_label, level, welsh_label"
            )
            raise exceptions.StopEtlPipelineErrorException

        reader = csv.reader(rows)
        version = dataset_service.get_latest_version_number()

        logging.info(f"using version number: {version}")
        dataset_service.update_status("subjects", "in progress")

        # add subject docs to new collection
        load_collection(
            COSMOS_DATABASE_URI, COSMOS_DATABASE_KEY, COSMOS_DATABASE_ID, COSMOS_COLLECTION_SUBJECTS, reader, version
        )

        logging.info(f"Successfully loaded in {number_of_subjects} subject documents")
        dataset_service.update_status("subjects", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"SubjectBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        dataset_service.update_status("subjects", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at SubjectBuilder" + msgin.get_body().decode("utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        logging.error(f"SubjectBuilder failed on {function_fail_datetime} ", exc_info=True)

        # Raise to Azure
        raise e
