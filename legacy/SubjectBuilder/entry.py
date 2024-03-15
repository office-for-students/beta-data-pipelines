import csv
import logging
from datetime import datetime

from decouple import config

from legacy.SubjectBuilder.database import load_collection
from legacy.SubjectBuilder.validate import column_headers
from legacy.services import exceptions
from legacy.services.blob import BlobService
from legacy.services.dataset_service import DataSetService


def subject_builder_main() -> None:
    msgerror = ""

    dsh = DataSetService()

    logging.info(f"SubjectBuilder message queue triggered")

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

    # mail_helper = MailHelper()
    environment = config("ENVIRONMENT")

    logging.info(
        f"SubjectBuilder function started on {function_start_datetime}"
    )

    cosmosdb_uri = config("COSMOS_DATABASE_URI")
    cosmosdb_key = config("COSMOS_DATABASE_KEY")
    db_id = config("COSMOS_DATABASE_ID")
    collection_id = config("COSMOS_COLLECTION_SUBJECTS")

    try:
        blob_helper = BlobService()

        # Read the Blob into a BytesIO object
        storage_container_name = config("BLOB_SUBJECTS_CONTAINER_NAME")
        storage_blob_name = config("BLOB_SUBJECTS_BLOB_NAME")

        csv_string = blob_helper.get_str_file(storage_container_name, storage_blob_name)

        rows = csv_string.splitlines()
        number_of_subjects = len(rows) - 1

        # csv header row
        if not column_headers(rows[0]):
            logging.error(
                "file headers are incorrect, expecting the following: code, english_label, level, welsh_label"
            )
            raise exceptions.StopEtlPipelineErrorException

        reader = csv.reader(rows)
        version = dsh.get_latest_version_number()

        logging.info(f"using version number: {version}")
        dsh.update_status("subjects", "in progress")

        # add subject docs to new collection
        load_collection(
            cosmosdb_uri, cosmosdb_key, db_id, collection_id, reader, version
        )

        logging.info(f"Successfully loaded in {number_of_subjects} subject documents")
        dsh.update_status("subjects", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"SubjectBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        dsh.update_status("subjects", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at SubjectBuilder" + msgin.get_body().decode("utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        logging.error(f"SubjectBuilder failed on {function_fail_datetime} ", exc_info=True)

        # Raise to Azure
        raise e
