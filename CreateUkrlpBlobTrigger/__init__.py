"""Contains the entry point for Create UKRLP lookups Azure Function implementation"""

from datetime import datetime
import logging
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
        logging.info(f"CreateUkrlpBlobTrigger creating UKRLP lookups\n"
                     f"Name: {xmlblob.name}\n"
                     f"Blob Size: {xmlblob.length} bytes")

        create_ukrlp_start_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.info('CreateUkrlp function started on ' + pipeline_start_datetime)

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the data
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode('utf-8')

        # TODO Validate the HESA XML

        # Parse the xml and create the lookups
        lookup_creator = LookupCreator(xml_string)
        lookup_creator.create_ukrlp_lookups()


        #
        # Copy the compressed HESA XML to the Blob storage monitored by Etl pipeline
        #

        storage_account_name = os.environ['AzureStorageAccountName']
        storage_account_key = os.environ['AzureStorageAccountKey']
        output_container_name = os.environ['EtlContainerName']
        output_blob_name_prefix = os.environ['OutputBlobNamePrefix']
        source_url = os.environ['UkrlpBlobUrl']

        # Instantiate the Block Blob Service
        blob_service = BlockBlobService(
            account_name = storage_account_name, 
            account_key = storage_account_key)
        logging.info("Created Block Blob Service to Azure Storage Account '%s'", 
            storage_account_name)

        # Copy the dummy raw HESA XML from the dummy to the landing BLOB container
        # Prefix the output BLOB name with the datetime it is being ingested

        blob_service.copy_blob(
            container_name = output_container_name, 
            blob_name = output_blob_name_prefix + create_ukrlp_start_datetime + '-' + dummy_input_blob_name,
            copy_source = source_url)
        logging.info("Ingested raw HESA XML into Azure Storage Account Container '%s'", 
            output_container_name)

        pipeline_end_datetime = datetime.today().strftime('%Y%m%d %H%M%S')
        logging.info('CreateUkrlp successfully finished on ' + pipeline_end_datetime)

    except Exception as e:
        # Unexpected exception
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
