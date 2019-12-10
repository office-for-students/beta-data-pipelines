#!/usr/bin/env python

""" EtlPipelineBlobTrigger: Execute the ETL pipeline based on a BLOB trigger """

import gzip
import io
import logging
import os
from datetime import datetime
from distutils.util import strtobool

import azure.functions as func
from azure.storage.blob import BlockBlobService

from SharedCode.blob_helper import BlobHelper
from SharedCode.dataset_helper import DataSetHelper
from SharedCode import exceptions

from . import course_docs, validators


def main(msgin: func.QueueMessage, msgout: func.Out[str]):

    """ Master ETL Pipeline - note that currently, the end-to-end ETL pipeline is
    executed via this single Azure Function which calls other Python functions
    embedded within the same deployment codebase (see imports above).
    TO DO: Investigate if/how this pipeline can be broken down into individual
    Azure Functions chained/integrated and orchestrated using Azure Data Factory
    and/or Function App. """

    try:

        dsh = DataSetHelper()

        logging.info(
            f"EtlPipelineBlobTrigger message queue triggered\n"
        )

        function_start_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"EtlPipelineBlobTrigger function started on {function_start_datetime}"
        )

        """ DECOMPRESSION - Decompress the compressed HESA XML """
        # Note the HESA XML blob provided to this function will be gzip compressed.
        # This is a work around for a limitation discovered in Azure,
        # in that Functions written in Python do not get triggered
        # correctly with large blobs. Tests showed this is not a limitation
        # with Funtions written in C#.

        blob_helper = BlobHelper()

        xml_string = blob_helper.get_hesa_xml()

        """ LOADING - Parse XML and load enriched JSON docs to database """

        version = dsh.get_latest_version_number()
        dsh.update_status("courses", "in progress")
        course_docs.load_course_docs(xml_string, version)
        dsh.update_status("courses", "succeeded")

        function_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"EtlPipelineBlobTrigger successfully finished on {function_end_datetime}"
        )

        msgout.set(f"EtlPipelineBlobTrigger successfully finished on {function_end_datetime}")

    except exceptions.StopEtlPipelineWarningException:

        dsh.update_status("courses", "failed")
        # A WARNING is raised during the ETL Pipeline and StopEtlPipelineOnWarning=True
        # For example, the incoming raw XML is not valid against its XSD
        error_message = (
            "A WARNING has been encountered during the ETL Pipeline. "
            "The Pipeline will be stopped since StopEtlPipelineOnWarning has been "
            "set to TRUE in the Application Settings."
        )
        logging.error(error_message)
        pipeline_fail_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.error(f"EtlPipelineBlobTrigger failed on {pipeline_fail_datetime}")
        raise Exception(error_message)
