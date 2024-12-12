#!/usr/bin/env python

""" EtlPipeline: Execute the ETL pipeline based on a message queue trigger """

import logging
import traceback
from datetime import datetime
from typing import Any

from constants import BLOB_HESA_BLOB_NAME
from constants import BLOB_HESA_CONTAINER_NAME
from legacy.EtlPipeline import course_docs


def etl_pipeline_main(
        blob_service: type['BlobServiceBase'],
        dataset_service: type['DataSetServiceBase'],
        cosmos_service: type['CosmosServiceBase']
) -> dict[str, Any]:
    response = {}

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

        xml_string = blob_service.get_str_file(BLOB_HESA_CONTAINER_NAME, BLOB_HESA_BLOB_NAME)

        version = dataset_service.get_latest_version_number()

        """ LOADING - Parse XML and load enriched JSON docs to database """

        dataset_service.update_status("courses", "in progress")

        course_docs.load_course_docs(
            xml_string=xml_string,
            version=version,
            blob_service=blob_service,
            cosmos_service=cosmos_service,
            dataset_service=dataset_service
        )
        dataset_service.update_status("courses", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        message = f"EtlPipeline successfully finished on {function_end_datetime}"

        logging.info(message)
        response["message"] = message
        response["statusCode"] = 200

    except Exception as e:

        dataset_service.update_status("courses", "failed")
        # A WARNING is raised during the ETL Pipeline and StopEtlPipelineOnWarning=True
        # For example, the incoming raw XML is not valid against its XSD

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")


        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at EtlPipeline" + msgin.get_body().decode(
        #         "utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        message = f"EtlPipeline failed on {function_fail_datetime}"
        logging.error(message)
        response["message"] = message
        response["exception"] = traceback.format_exc()
        response["statusCode"] = 500

        # raise e

    return response
