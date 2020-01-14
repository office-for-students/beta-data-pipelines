import logging
import os
import io
import csv

from datetime import datetime

import azure.functions as func

from SharedCode.blob_helper import BlobHelper
from SharedCode.mail_helper import MailHelper

from . import validate, database, exceptions


def main(req: func.HttpRequest,) -> None:
    logging.info(f"SubjectBuilder request triggered")

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
    function_start_date = datetime.today().strftime("%d.%m.%Y")

    mail_helper = MailHelper()
    environment = os.environ["Environment"]
    mail_helper.send_message(f"Subject builder started on {function_start_datetime}", f"Subject Builder {environment} - {function_start_date} - Started")

    logging.info(
        f"SubjectBuilder function started on {function_start_datetime}"
    )

    cosmosdb_uri = os.environ["AzureCosmosDbUri"]
    cosmosdb_key = os.environ["AzureCosmosDbKey"]
    throughput = os.environ["DatabaseThroughput"]
    db_id = os.environ["AzureCosmosDbDatabaseId"]
    collection_id = os.environ["AzureCosmosDbSubjectsCollectionId"]

    try:
        blob_helper = BlobHelper()

        # Read the Blob into a BytesIO object
        storage_container_name = os.environ["AzureStorageAccountSubjectsContainerName"]
        storage_blob_name = os.environ["AzureStorageBlobName"]

        subject_file = io.BytesIO()

        blob_helper.blob_service.get_blob_to_stream(storage_container_name, storage_blob_name, subject_file, max_connections=1)

        subject_file.seek(0)

        csv_bytes = subject_file.read()

        # Decode the bytes into a string
        csv_string = csv_bytes.decode("utf-8-sig")

        rows = csv_string.splitlines()
        number_of_subjects = len(rows) - 1

        # csv header row
        if not validate.column_headers(rows[0]):
            logging.error(
                "file headers are incorrect, expecting the following: code, english_label, level, welsh_label"
            )
            raise exceptions.StopEtlPipelineErrorException

        reader = csv.reader(rows)

        # delete and recreate collection
        database.build_collection(
            cosmosdb_uri, cosmosdb_key, int(throughput), db_id, collection_id
        )

        # add subject docs to new collection
        database.load_collection(
            cosmosdb_uri, cosmosdb_key, db_id, collection_id, reader
        )

        logging.info(f"Successfully loaded in {number_of_subjects} subject documents")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_end_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Subject builder completed on {function_end_datetime}", f"Subject Builder {environment} - {function_end_date} - Completed")

        logging.info(
            f"SubjectBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Search builder failed on {function_fail_datetime} at EtlPipeline", f"Search Builder {environment} - {function_fail_date} - Failed")

        logging.error(f"SubjectBuilder failed on {function_fail_datetime} ", exc_info=True)

        # Raise to Azure
        raise e
