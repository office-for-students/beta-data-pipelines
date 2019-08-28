#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import gzip
import io
import logging
import os
from datetime import datetime

import azure.functions as func

from SharedCode.exceptions import (
    XmlValidationError,
    StopEtlPipelineErrorException,
)
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

        """ CREATE NEW DATASET """
        data_set_creator = DataSetCreator()
        data_set_creator.load_new_dataset_doc()

        logging.info("CreateDataSetBlobTrigger successfully finished.")

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
