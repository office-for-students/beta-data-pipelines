#!/usr/bin/env python
import gzip
import io
#import logging
import os
from datetime import datetime

import azure.functions as func

from SharedCode import exceptions
from SharedCode.dataset_helper import DataSetHelper
from SharedCode.blob_helper import BlobHelper

from .institution_docs import InstitutionDocs


def main(xmlblob: func.InputStream, context: func.Context):

    try:
        dsh = DataSetHelper()

        #logging.info(
        #    f"CreateInstBlobTrigger processing BLOB \n"
        #    f"Name: {xmlblob.name}\n"
        #    f"Blob Size: {xmlblob.length} bytes"
        #)

        """ PREPARATION """
        xsd_filename = os.environ["XsdFilename"]
        xsd_path = os.path.join(context.function_directory, xsd_filename)

        #logging.info(
        #    f"CreateInstBlobTrigger configuration values \n"
        #    f"XsdFilename: {xsd_filename}\n"
        #    f"XsdPath: {xsd_path}"
        #)

        """ DECOMPRESSION - Decompress the compressed HESA XML """
        # The XML blob provided to this function will be gzip compressed.
        # This is a work around for a limitation discovered in Azure,
        # where Functions written in Python do not get triggered # correctly with large blobs. Tests showed this is not a limitation
        # with Funtions written in C#.

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the data
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode("utf-8")

        """ LOADING - extract data and load JSON Documents """

        version = dsh.get_latest_version_number()
        #logging.info(f"using version number: {version}")
        dsh.update_status("institutions", "in progress")

        inst_docs = InstitutionDocs(xml_string)
        inst_docs.create_institution_docs(version)
        dsh.update_status("institutions", "succeeded")

        """ PASS THE COMPRESSED XML TO NEXT AZURE FUNCTION IN THE PIPELINE"""
        destination_container_name = os.environ["EtlInputContainerName"]
        blob_helper = BlobHelper(xmlblob)
        blob_helper.create_output_blob(destination_container_name)

        #logging.info("CreateInstBlobTrigger successfully finished.")

    except exceptions.StopEtlPipelineWarningException:

        # A WARNING is raised while the function is running and
        # StopEtlPipelineOnWarning=True. For example, the incoming raw XML
        # is not valid against its XSD
        error_message = (
            "A WARNING has been encountered while the function is running. "
            "The function will be stopped since StopEtlPipelineOnWarning is "
            "set to TRUE in the Application Settings."
        )
        #logging.error(error_message)
        #logging.error("CreateInstBlobTrigger stopped")
        dsh.update_status("institutions", "failed")
        raise Exception(error_message)

    except Exception as e:
        # Unexpected exception
        dsh.update_status("institutions", "failed")
        #logging.error(
        #    "CreateInstBlogTrigger unexpected exception ", exc_info=True
        #)

        # Raise to Azure
        raise e
