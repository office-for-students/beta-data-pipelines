#!/usr/bin/env python
import gzip
import io
import logging
import os
from datetime import datetime

import azure.functions as func

from SharedCode import exceptions


def main(xmlblob: func.InputStream, context: func.Context):

    logging.info(
        f"CreateDataSetBlobTrigger processing BLOB \n"
        f"Name: {xmlblob.name}\n"
        f"Blob Size: {xmlblob.length} bytes"
    )
    try:

        """ DECOMPRESSION - Decompress the compressed HESA XML """

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the data
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode("utf-8")

        """ CREATION - Create a new dataset entry """

"""
        pipeline_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            "CreateInstBlobTrigger successfully finished on "
            + pipeline_end_datetime
        )

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
        logging.error("CreateInstBlobTrigger stopped")
        raise Exception(error_message)
"""

    except Exception as e:
        # Unexpected exception
        logging.error(
            "CreateInstBlogTrigger unexpected exception ", exc_info=True
        )

        # Raise to Azure
        raise e
