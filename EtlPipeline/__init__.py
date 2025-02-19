#!/usr/bin/env python

""" EtlPipeline: Execute the ETL pipeline based on a message queue trigger """

import logging
import os
from datetime import datetime

import azure.functions as func

from EtlPipeline import course_docs
from SharedCode.blob_helper import BlobHelper
from SharedCode.dataset_helper import DataSetHelper
# from SharedCode.mail_helper import MailHelper


def main(msgin: func.QueueMessage, msgout: func.Out[str]):
    """ Master ETL Pipeline - note that currently, the end-to-end ETL pipeline is
    executed via this single Azure Function which calls other Python functions
    embedded within the same deployment codebase (see imports above).
    TO DO: Investigate if/how this pipeline can be broken down into individual
    Azure Functions chained/integrated and orchestrated using Azure Data Factory
    and/or Function App. """

    # TODO: apw: Ensure that UseLocalTestXMLFile is set to false in local.settings.json before going live.
    use_local_test_XML_file = os.environ.get('UseLocalTestXMLFile')

    msgerror = ""

    # mail_helper = MailHelper()
    environment = os.environ["Environment"]

    dsh = DataSetHelper()

    try:

        logging.info(
            f"EtlPipeline message queue triggered\n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"EtlPipeline function started on {function_start_datetime}"
        )

        """ DECOMPRESSION - Decompress the compressed HESA XML """
        # Note the HESA XML blob provided to this function will be gzip compressed.
        # This is a work around for a limitation discovered in Azure,
        # in that Functions written in Python do not get triggered
        # correctly with large blobs. Tests showed this is not a limitation
        # with Funtions written in C#.

        blob_helper = BlobHelper()

        storage_container_name = os.environ["AzureStorageHesaContainerName"]
        storage_blob_name = os.environ["AzureStorageHesaBlobName"]

        if use_local_test_XML_file:
            mock_xml_source_file = open(os.environ["LocalTestXMLFile"], "r")
            xml_string = mock_xml_source_file.read()
        else:
            xml_string = blob_helper.get_str_file(storage_container_name, storage_blob_name)

        version = dsh.get_latest_version_number()

        """ LOADING - Parse XML and load enriched JSON docs to database """

        dsh.update_status("courses", "in progress")
        course_docs.load_course_docs(xml_string, version)
        dsh.update_status("courses", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"EtlPipeline successfully finished on {function_end_datetime}"
        )

        msgout.set(msgin.get_body().decode("utf-8") + msgerror)

    except Exception as e:

        dsh.update_status("courses", "failed")
        # A WARNING is raised during the ETL Pipeline and StopEtlPipelineOnWarning=True
        # For example, the incoming raw XML is not valid against its XSD

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at EtlPipeline" + msgin.get_body().decode(
        #         "utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        logging.error(f"EtlPipeline failed on {function_fail_datetime}")

        raise e
