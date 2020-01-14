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
from SharedCode.mail_helper import MailHelper
from .dataset_creator import DataSetCreator
from . import validators


def main(req: func.HttpRequest, msgout: func.Out[str]) -> None:

    logging.info(
        f"CreateDataSet timer triggered\n"
    )

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
    function_start_date = datetime.today().strftime("%d.%m.%Y")

    mail_helper = MailHelper()
    mail_helper.send_message(f"Automated data import started on {function_start_datetime}", f"Data Import - {function_start_date} - Started")

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

            function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
            function_fail_date = datetime.today().strftime("%d.%m.%Y")

            mail_helper = MailHelper()
            mail_helper.send_message(f"Automated data import failed on {function_fail_datetime} at EtlPipeline", f"Data Import - {function_fail_date} - Failed")

            logging.info(f"CreateDataSet failed on {function_fail_datetime}")
            return

        function_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CreateDataSet successfully finished on {function_end_datetime}"
        )

        msgout.set(f"CreateDataSet successfully finished on {function_end_datetime}")

    except StopEtlPipelineErrorException as e:
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper = MailHelper()
        mail_helper.send_message(f"Automated data import failed on {function_fail_datetime} at EtlPipeline", f"Data Import - {function_fail_date} - Failed")

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
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper = MailHelper()
        mail_helper.send_message(f"Automated data import failed on {function_fail_datetime} at EtlPipeline", f"Data Import - {function_fail_date} - Failed")

        logging.error(
            f"CreateDataSet failed on {function_fail_datetime}",
            exc_info=True,
        )
        # Raise to Azure
        raise e
