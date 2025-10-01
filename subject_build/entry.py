import logging
import traceback
from datetime import datetime
from typing import Any

from constants import BLOB_SUBJECTS_BLOB_NAME
from constants import BLOB_SUBJECTS_CONTAINER_NAME
from constants import COSMOS_COLLECTION_SUBJECTS
from subject_build.database import load_subject_documents
from subject_build.validate import column_headers
from services import exceptions
from etl_pipeline.utils import process_csv_string


def subject_builder_main(
        blob_service: type['BlobServiceBase'],
        dataset_service: type['DataSetServiceBase'],
        cosmos_service: type['CosmosServiceBase']
) -> dict[str, Any]:
    """
    Builds subject dataset using the CSV stored in the subjects blob

    :return: None
    """
    response = {}
    msgerror = ""

    logging.info(f"SubjectBuilder message queue triggered")

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

    # mail_helper = MailHelper()

    logging.info(
        f"SubjectBuilder function started on {function_start_datetime}"
    )

    try:
        # Read the Blob into a BytesIO object
        csv_string = blob_service.get_str_file(
            container_name=BLOB_SUBJECTS_CONTAINER_NAME,
            blob_name=BLOB_SUBJECTS_BLOB_NAME
        )

        rows = csv_string.splitlines()
        number_of_subjects = len(rows) - 1
        rows_list = process_csv_string(csv_string)

        # csv header row
        try:
            if not column_headers(rows[0]):
                logging.error(
                    "file headers are incorrect, expecting the following: code, english_label, level, welsh_label"
                )
                raise exceptions.StopEtlPipelineErrorException
        except IndexError:
            logging.error(
                f"Subjects CSV empty or not present, please ensure the subjects CSV '{BLOB_SUBJECTS_BLOB_NAME}' is "
                f"present inside the container '{BLOB_SUBJECTS_CONTAINER_NAME}'"
            )
            raise exceptions.StopEtlPipelineErrorException

        # reader = csv.reader(rows)
        version = dataset_service.get_latest_version_number()

        logging.info(f"using version number: {version}")
        dataset_service.update_status("subjects", "in progress")
        # add subject docs to new collection
        load_subject_documents(
            rows=rows_list,
            version=version,
            collection_link=cosmos_service.get_collection_link(
                COSMOS_COLLECTION_SUBJECTS
            ),
            collection_container=cosmos_service.get_container(
                container_id=COSMOS_COLLECTION_SUBJECTS
            )
        )

        logging.info(f"Successfully loaded in {number_of_subjects} subject documents")
        dataset_service.update_status("subjects", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        message = f"SubjectBuilder successfully finished on {function_end_datetime}"
        logging.info(message)
        response["message"] = message
        response["statusCode"] = 200

    except Exception as e:
        # Unexpected exception
        dataset_service.update_status("subjects", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at subject_build" + msgin.get_body().decode("utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        message = f"SubjectBuilder failed on {function_fail_datetime} "
        logging.error(message, exc_info=True)
        response["message"] = message
        response["exception"] = traceback.format_exc()
        response["statusCode"] = 500

        # Raise to Azure
        # raise e

    return response
