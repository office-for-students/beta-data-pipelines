import logging
import os
import io
import gzip
import traceback

import azure.functions as func

from .ukrlp_lookups import create_ukrlp_lookups


def main(xmlblob: func.InputStream):
    """Creates the UKRLP lookup tables for later enrichment
    

    This Azure Function carries out the following steps:
    *. Decompresses the XML HESA DataSet
    *. Parses the INSTITUTION data from the DataSet
    *. Retrieves enrichment data from the UKRLP API for each institution
    *. Creates a lookup item for each Institution and writes it to CosmosDB 

    """

    try:
        logging.info(f"CreateUkrlpBlobTrigger creating UKRLP lookups\n"
                     f"Name: {xmlblob.name}\n"
                     f"Blob Size: {xmlblob.length} bytes")

        # Read the compressed Blob into a BytesIO object
        compressed_file = io.BytesIO(xmlblob.read())

        # Read the compressed file into a GzipFile object
        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        # Decompress the data
        decompressed_file = compressed_gzip.read()

        # Decode the bytes into a string
        xml_string = decompressed_file.decode('utf-8')

        # TODO Validate the HESA XML

        # Parse the xml and create the lookups
        create_ukrlp_lookups(xml_string)

    except Exception as e:
        # Unexpected exception
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
