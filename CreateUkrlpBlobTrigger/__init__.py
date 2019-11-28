"""Contains the entry point for Create UKRLP lookups Azure Function implementation"""

from datetime import datetime
#import logging
import os
import io
import gzip
import traceback

import azure.functions as func

from azure.storage.blob import BlockBlobService

from .lookup_creator import LookupCreator


def main(xmlblob: func.InputStream):
    """Creates the UKRLP lookup tables for later use

    This Azure Function carries out the following steps:
    * Decompresses the XML HESA DataSet

    * Parses the INSTITUTION data from the DataSet

    * Retrieves enrichment data from the UKRLP API for each institution

    * Creates a lookup item for each Institution and writes it to CosmosDB

    * Currently, once completed successfully this function triggers the Etl function by copying
      the compressed XML passed in to a Blob storage monitored by the Etl function.

    """

    try:
        #logging.info(
            f"CreateUkrlpBlobTrigger creating UKRLP lookups\n"
            f"Name: {xmlblob.name}\n"
            f"Blob Size: {xmlblob.length} bytes"
        )

        create_ukrlp_start_datetime = datetime.today().strftime(
            "%Y%m%d %H%M%S"
        )

        #logging.info(
            f"CreateUkrlp function started on {create_ukrlp_start_datetime}"
        )

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the data
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode("utf-8")

        # Parse the xml and create the lookups
        lookup_creator = LookupCreator(xml_string)
        lookup_creator.create_ukrlp_lookups()

        #
        # Copy the compressed HESA XML to the Blob storage monitored by Etl pipeline
        #
        storage_account_name = os.environ["AzureStorageAccountName"]
        storage_account_key = os.environ["AzureStorageAccountKey"]

        # Instantiate the Block Blob Service
        blob_service = BlockBlobService(
            account_name=storage_account_name, account_key=storage_account_key
        )

        #logging.info(
            "Created Block Blob Service to Azure Storage Account {storage_account_name}"
        )

        # Copy the dummy HESA XML we've just processed to the Institution input BLOB container
        output_container_name = os.environ["InstInputContainerName"]
        dummy_etl_blob_name = os.environ["DummyEtlBlobName"]
        source_url = os.environ["CreateUkrlpSourceUrl"]

        source_url += xmlblob.name
        blob_filename = xmlblob.name.split("/")[1]
        destination_blob_name = (
            f"{create_ukrlp_start_datetime}-{blob_filename}"
        )
        #logging.info(
            f"Copy the XML we have processed to {destination_blob_name}"
        )

        blob_service.copy_blob(
            container_name=output_container_name,
            blob_name=destination_blob_name,
            copy_source=source_url,
        )

        create_ukrlp_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        #logging.info(
            f"CreateUkrlp successfully finished on {create_ukrlp_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        #logging.error("Unexpected exception")
        #logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
