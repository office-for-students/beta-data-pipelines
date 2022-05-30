#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """
import sys
import os
# print python version
print(f"Python version: {sys.version}")
# print os version
print(f"OS version: {os.name}")

#print the python path
print(f"Python path: {sys.path}")


import logging
import os
from datetime import datetime

import azure.functions as func

from SharedCode.blob_helper import BlobHelper
from SharedCode.exceptions import DataSetTooEarlyError
from SharedCode.exceptions import StopEtlPipelineErrorException
from SharedCode.exceptions import XmlValidationError
from SharedCode.mail_helper import MailHelper
from . import validators
from .dataset_creator import DataSetCreator


def main(req: func.HttpRequest, msgout: func.Out[str]) -> None:
    # TODO: apw: Ensure that UseLocalTestXMLFile is set to false in local.settings.json before going live.
    use_local_test_XML_file = os.environ.get('UseLocalTestXMLFile')

    msgerror = ""

    logging.info(
        f"CreateDataSet timer triggered\n"
    )

    function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
    function_start_date = datetime.today().strftime("%d.%m.%Y")

    mail_helper = MailHelper()
    environment = os.environ["Environment"]
    mail_helper.send_message(
        f"Automated data import started on {function_start_datetime}",
        f"Data Import {environment} - {function_start_date} - Started"
    )

    logging.info(
        f"CreateDataSet function started on {function_start_datetime}"
    )

    try:
        blob_helper = BlobHelper()

        storage_container_name = os.environ["AzureStorageHesaContainerName"]
        storage_blob_name = os.environ["AzureStorageHesaBlobName"]

        if use_local_test_XML_file:
            mock_xml_source_file = open(os.environ["LocalTestXMLFile"],"r")
            xml_string = mock_xml_source_file.read()
        else:
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
            function_fail_date = datetime.today().strftime("%d.%m.%Y")

            mail_helper.send_message(
                f"Automated data import failed on {function_fail_datetime} at CreateDataSet" + msgerror,
                f"Data Import {environment} - {function_fail_date} - Failed"
            )

            logging.info(f"CreateDataSet failed on {function_fail_datetime}")
            return

        function_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CreateDataSet successfully finished on {function_end_datetime}"
        )

        msgout.set(msgerror + "\n")

    except StopEtlPipelineErrorException as e:
        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(
            f"Automated data import failed on {function_fail_datetime} at CreateDataSet" + f"{msgerror} {e}",
            f"Data Import {environment} - {function_fail_date} - Failed"
        )

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

        mail_helper.send_message(
            f"Automated data import failed on {function_fail_datetime} at CreateDataSet" + msgerror,
            f"Data Import {environment} - {function_fail_date} - Failed"
        )

        logging.error(
            f"CreateDataSet failed on {function_fail_datetime}",
            exc_info=True,
        )
        # Raise to Azure
        raise e
