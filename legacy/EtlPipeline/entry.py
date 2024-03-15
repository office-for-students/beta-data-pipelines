#!/usr/bin/env python

""" EtlPipeline: Execute the ETL pipeline based on a message queue trigger """

import logging
from datetime import datetime

from legacy.EtlPipeline import course_docs
from legacy.services.blob import BlobService
from legacy.services.dataset_service import DataSetService


def etl_pipeline_main(
        blob_service: BlobService,
        hesa_container_name: str,
        hesa_blob_name: str,
        local_test_xml_file=None
) -> None:
    dsh = DataSetService()

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

        if local_test_xml_file:
            mock_xml_source_file = open(local_test_xml_file, "r")
            xml_string = mock_xml_source_file.read()
        else:
            xml_string = blob_service.get_str_file(hesa_container_name, hesa_blob_name)

        version = dsh.get_latest_version_number()

        """ LOADING - Parse XML and load enriched JSON docs to database """

        dsh.update_status("courses", "in progress")

        course_docs.load_course_docs(
            xml_string,
            version,
            "COSMOS_DATABASE_ID",
            "COSMOS_COLLECTION_SUBJECTS",
            "COSMOS_COLLECTION_COURSES"
        )
        dsh.update_status("courses", "succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"EtlPipeline successfully finished on {function_end_datetime}"
        )

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
