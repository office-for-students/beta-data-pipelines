#!/usr/bin/env python
import gzip
import io
import logging
import os
from datetime import datetime

import azure.functions as func

from SharedCode import exceptions
from SharedCode.dataset_helper import DataSetHelper
from SharedCode.blob_helper import BlobHelper
from SharedCode.mail_helper import MailHelper

from .institution_docs import InstitutionDocs


def main(msgin: func.QueueMessage, msgout: func.Out[str]):

    mail_helper = MailHelper()
    environment = os.environ["Environment"]

    try:
        dsh = DataSetHelper()

        logging.info(
            f"CreateInst message queue triggered\n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CreateInst function started on {function_start_datetime}"
        )

        """ DECOMPRESSION - Decompress the compressed HESA XML """
        # The XML blob provided to this function will be gzip compressed.
        # This is a work around for a limitation discovered in Azure,
        # where Functions written in Python do not get triggered # correctly with large blobs. Tests showed this is not a limitation
        # with Funtions written in C#.

        blob_helper = BlobHelper()

        xml_string = blob_helper.get_hesa_xml()

        """ LOADING - extract data and load JSON Documents """

        version = dsh.get_latest_version_number()
        logging.info(f"using version number: {version}")
        dsh.update_status("institutions", "in progress")

        inst_docs = InstitutionDocs(xml_string)
        inst_docs.create_institution_docs(version)
        dsh.update_status("institutions", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CreateInst successfully finished on {function_end_datetime}"
        )

        msgout.set(f"CreateInst successfully finished on {function_end_datetime}")

    except exceptions.StopEtlPipelineWarningException:

        # A WARNING is raised while the function is running and
        # StopEtlPipelineOnWarning=True. For example, the incoming raw XML
        # is not valid against its XSD
        error_message = (
            "A WARNING has been encountered while the function is running. "
            "The function will be stopped since StopEtlPipelineOnWarning is "
            "set to TRUE in the Application Settings."
        )
        logging.error(error_message)

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Automated data import failed on {function_fail_datetime} at CreateInst", f"Data Import {environment} - {function_fail_date} - Failed")

        logging.error(f"CreateInst failed on {function_fail_datetime}")
        dsh.update_status("institutions", "failed")
        raise Exception(error_message)

    except Exception as e:
        # Unexpected exception
        dsh.update_status("institutions", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Automated data import failed on {function_fail_datetime} at CreateInst", f"Data Import {environment} - {function_fail_date} - Failed")

        logging.error(
            f"CreateInst failed on {function_fail_datetime}", exc_info=True
        )

        # Raise to Azure
        raise e
