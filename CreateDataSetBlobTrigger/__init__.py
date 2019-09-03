#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import gzip
import io
import logging

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

    logging.info(
        f"CreateDataSetBlobTrigger processing BLOB \n"
        f"Name: {xmlblob.name}\n"
        f"Blob Size: {xmlblob.length} bytes"
    )
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
            logging.error("Error unable to parse the XML data from HESA.")
            raise StopEtlPipelineErrorException

        """ CREATE NEW DATASET """
        data_set_creator = DataSetCreator()
        try:
            data_set_creator.load_new_dataset_doc()
        except DataSetTooEarlyError:
            logging.error("It's too soon to create another DataSet.")
            error_message = (
                "See the documentation for information on the environment "
                "variable that controls how frequently new DataSets "
                "can be created. "
            )
            logging.error(error_message)
            logging.info("CreateDataSetBlobTrigger is being stopped.")
            return

        logging.info("CreateDataSetBlobTrigger successfully finished.")

        """ PASS THE COMPRESSED XML TO NEXT AZURE FUNCTION IN THE PIPELINE"""
        blob_helper = BlobHelper(xmlblob)
        blob_helper.create_output_blob()

    except StopEtlPipelineErrorException as e:
        logging.error(
            "CreateDataSetBlogTrigger an error has stopped the pipeline",
            exc_info=True,
        )
        error_message = (
            "An ERROR has been encountered during "
            "CreateDataSetBlobTrigger. "
            "The CreateDataSetBlobTrigger will be stopped."
        )
        raise Exception(error_message)

    except Exception as e:
        logging.error(
            "CreateDataSetBlogTrigger unexpected exception raised",
            exc_info=True,
        )
        # Raise to Azure
        raise e
