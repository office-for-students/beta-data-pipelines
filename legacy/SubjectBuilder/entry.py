import csv
import logging
from datetime import datetime
from typing import Type

from constants import BLOB_SUBJECTS_BLOB_NAME
from constants import BLOB_SUBJECTS_CONTAINER_NAME
from legacy.SubjectBuilder.database import load_collection
from legacy.SubjectBuilder.validate import column_headers
from services import exceptions
from services.blob_service.base import BlobServiceBase
from services.cosmosservice import CosmosService
from services.dataset_service import DataSetService


def subject_builder_main(
        blob_service: Type['BlobServiceBase'],
        dataset_service: DataSetService,
        cosmos_service: CosmosService
) -> None:
    """
    Builds subject dataset using the CSV stored in the subjects blob

    :return: None
    """
    msgerror = ""

    logging.info(f"SubjectBuilder message queue triggered")

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

    # mail_helper = MailHelper()

    logging.info(
        f"SubjectBuilder function started on {function_start_datetime}"
    )

    try:
        # Read the Blob into a BytesIO object
        csv_string = blob_service.get_str_file(BLOB_SUBJECTS_CONTAINER_NAME, BLOB_SUBJECTS_BLOB_NAME)

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
            rows=reader,
            version=version,
            cosmos_service=cosmos_service
        )

        logging.info(f"Successfully loaded in {number_of_subjects} subject documents")
        dataset_service.update_status("subjects", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(f"SubjectBuilder successfully finished on {function_end_datetime}")

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
