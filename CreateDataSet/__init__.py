#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import logging
import os
from datetime import datetime

import azure.functions as func

from SharedCode.blob_helper import BlobHelper
from SharedCode.exceptions import DataSetTooEarlyError
from SharedCode.exceptions import StopEtlPipelineErrorException
from SharedCode.exceptions import XmlValidationError
from . import validators
from .dataset_creator import DataSetCreator


def main(req: func.HttpRequest, msgout: func.Out[str]) -> None:
    msgerror = ""

    logging.info(
        f"CreateDataSet timer triggered\n"
    )

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")


    logging.info(
        f"CreateDataSet function started on {function_start_datetime}"
    )

    try:
        blob_helper = BlobHelper()

        storage_container_name = os.environ["AzureStorageHesaContainerName"]
        storage_blob_name = os.environ["AzureStorageHesaBlobName"]

        xml_string = blob_helper.get_str_file(storage_container_name, storage_blob_name)

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

            function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

            logging.info(f"CreateDataSet failed on {function_fail_datetime}")
            return

        function_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CreateDataSet successfully finished on {function_end_datetime}"
        )

        msgout.set(msgerror + "\n")

    except StopEtlPipelineErrorException as e:
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.error(
            f"CreateDataSet failed on {function_fail_datetime}",
            exc_info=True,
        )
        error_message = (
            "An ERROR has been encountered during "
            "CreateDataSet. "
            "The CreateDataSet will be stopped."
        )
        raise Exception(error_message)
    except Exception as e:
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.error(
            f"CreateDataSet failed on {function_fail_datetime}",
            exc_info=True,
        )
        # Raise to Azure
        raise e