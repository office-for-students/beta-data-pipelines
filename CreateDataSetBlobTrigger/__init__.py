#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import gzip
import io
import os

import azure.functions as func

from SharedCode.exceptions import (
    XmlValidationError,
    StopEtlPipelineErrorException,
    DataSetTooEarlyError,
)
from SharedCode.blob_helper import BlobHelper
from .dataset_creator import DataSetCreator
from . import validators


def main(xmlblob: func.InputStream, context: func.Context):

    try:

        """ DECOMPRESSION - Decompress the compressed XML data"""
        compressed_file = io.BytesIO(xmlblob.read())

        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        decompressed_file = compressed_gzip.read()

        xml_string = decompressed_file.decode("utf-8")

        """ BASIC XML Validation """
        try:
            validators.parse_xml_data(xml_string)
        except XmlValidationError:
            raise StopEtlPipelineErrorException

        """ CREATE NEW DATASET """
        data_set_creator = DataSetCreator()
        try:
            data_set_creator.load_new_dataset_doc()
        except DataSetTooEarlyError:
            return

        """ PASS THE COMPRESSED XML TO NEXT AZURE FUNCTION IN THE PIPELINE"""
        destination_container_name = os.environ["UkrlpInputContainerName"]
        blob_helper = BlobHelper(xmlblob)
        blob_helper.create_output_blob(destination_container_name)

    except StopEtlPipelineErrorException as e:
        error_message = (
            "An ERROR has been encountered during "
            "CreateDataSetBlobTrigger. "
            "The CreateDataSetBlobTrigger will be stopped."
        )
        raise Exception(error_message)

    except Exception as e:
        # Raise to Azure
        raise e
