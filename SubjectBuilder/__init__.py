import logging
import os
import io
import csv

from datetime import datetime

import azure.functions as func

from SharedCode.dataset_helper import DataSetHelper
from SharedCode.blob_helper import BlobHelper
from SharedCode.mail_helper import MailHelper

from . import validate, database, exceptions


def main(msgin: func.QueueMessage, msgout: func.Out[str]):
    dsh = DataSetHelper()

    logging.info(f"SubjectBuilder message queue triggered")

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

    mail_helper = MailHelper()
    environment = os.environ["Environment"]

    logging.info(
        f"SubjectBuilder function started on {function_start_datetime}"
    )

    cosmosdb_uri = os.environ["AzureCosmosDbUri"]
    cosmosdb_key = os.environ["AzureCosmosDbKey"]
    db_id = os.environ["AzureCosmosDbDatabaseId"]
    collection_id = os.environ["AzureCosmosDbSubjectsCollectionId"]

    try:
        blob_helper = BlobHelper()

        # Read the Blob into a BytesIO object
        storage_container_name = os.environ["AzureStorageSubjectsContainerName"]
        storage_blob_name = os.environ["AzureStorageSubjectsBlobName"]

        csv_string = blob_helper.get_str_file(storage_container_name, storage_blob_name)

        rows = csv_string.splitlines()
        number_of_subjects = len(rows) - 1

        # csv header row
        if not validate.column_headers(rows[0]):
            logging.error(
                "file headers are incorrect, expecting the following: code, english_label, level, welsh_label"
            )
            raise exceptions.StopEtlPipelineErrorException

        reader = csv.reader(rows)

        version = dsh.get_latest_version_number()
        logging.info(f"using version number: {version}")
        dsh.update_status("subjects", "in progress")

        # add subject docs to new collection
        database.load_collection(
            cosmosdb_uri, cosmosdb_key, db_id, collection_id, reader, version
        )

        logging.info(f"Successfully loaded in {number_of_subjects} subject documents")
        dsh.update_status("subjects", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"SubjectBuilder successfully finished on {function_end_datetime}"
        )

        msgout.set(f"SubjectBuilder successfully finished on {function_end_datetime}")

    except Exception as e:
        # Unexpected exception
        dsh.update_status("subjects", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Subject builder failed on {function_fail_datetime}", f"Search Builder {environment} - {function_fail_date} - Failed")

        logging.error(f"SubjectBuilder failed on {function_fail_datetime} ", exc_info=True)

        # Raise to Azure
        raise e
