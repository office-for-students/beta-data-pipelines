#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import gzip
import io
import logging
import os

from datetime import datetime

import azure.functions as func

from azure.storage.blob import BlockBlobService

from SharedCode.exceptions import (
    XmlValidationError,
    StopEtlPipelineErrorException,
    DataSetTooEarlyError,
)
from SharedCode.blob_helper import BlobHelper
from .dataset_creator import DataSetCreator
from . import validators


def main(functimer: func.TimerRequest, msgout: func.Out[str]) -> None:

    logging.info(
        f"CreateDataSet timer triggered\n"
    )

    function_start_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
    logging.info(
        f"CreateDataSet function started on {function_start_datetime}"
    )

    try:
        blob_helper = BlobHelper()

        xml_string = blob_helper.get_hesa_xml()

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
            logging.info("CreateDataSet is being stopped.")
            return

        function_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CreateDataSet successfully finished on {function_end_datetime}"
        )

        msgout.set(f"CreateDataSet successfully finished on {function_end_datetime}")

    except StopEtlPipelineErrorException as e:
        logging.error(
            "CreateDataSet an error has stopped the pipeline",
            exc_info=True,
        )
        error_message = (
            "An ERROR has been encountered during "
            "CreateDataSet. "
            "The CreateDataSet will be stopped."
        )
        raise Exception(error_message)

    except Exception as e:
        logging.error(
            "CreateDataSet unexpected exception raised",
            exc_info=True,
        )
        # Raise to Azure
        raise e
