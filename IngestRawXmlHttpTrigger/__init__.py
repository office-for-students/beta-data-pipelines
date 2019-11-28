#!/usr/bin/env python

""" IngestRawXmlHttpTrigger: Request and ingest the raw HESA XML into an Azure Storage Account BLOB Container """

import os
import azure.functions as func
#import logging
import json

from .utils import get_url_from_req, OfsMissingUrlError

from azure.storage.blob import BlockBlobService
from datetime import datetime

__author__ = "Jillur Quddus, Nathan Shumoogum"
__credits__ = ["Jillur Quddus", "Nathan Shumoogum"]
__version__ = "0.1"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@methods.co.uk"
__status__ = "Development"

def main(req: func.HttpRequest) -> func.HttpResponse:
    #logging.info('IngestRawXmlHttpTrigger Python HTTP trigger function processed a request.')

    """ An Azure Function invoked by a HTTP Trigger that will make a HTTP request to 
    the HESA HTTP/REST endpoint in order to retrieve the raw XML data and thereafter
    ingest it into an Azure Storage Account and a BLOB container. """

    """ Update 10/05/2019: Since the HESA HTTP/REST endpoint is not yet defined 
    nor available, at this time this Azure Function will simply copy a static batch
    version of the raw XML data stored from one dummy BLOB container to the 
    landing BLOB container. Once the HESA HTTP/REST endpoint is defined and 
    available, this Azure Function will need to be factored/replaced. """

    try:

        # Microsoft recommend using Environmental Variable methods in order to read
        # the Azure Function Application Settings. See:
        #  https://docs.microsoft.com/en-us/azure/azure-functions/functions-dotnet-class-library

        storage_account_name = os.environ['AzureStorageAccountName']
        storage_account_key = os.environ['AzureStorageAccountKey']
        output_container_name = os.environ['AzureStorageAccountOutputContainerName']
        output_blob_name_prefix = os.environ['OutputBlobNamePrefix']
        dummy_input_container_name = os.environ['DummyAzureStorageAccountInputContainerName']
        dummy_input_blob_name = os.environ['DummyInputBlobName']
        dummy_input_blob_url = os.environ['DummyInputBlobUrl']
        

        # TODO: Uncomment the line below to get the resource_url sent by HESA.
        # I'm leaving this call commented out for now so can still trigger this Azure Function
        # easily from the browser while testing. When we do add the call in, remember to
        # also remove the get HTTP verb from function.json file as we should only accept post
        # requests. 

        # hesa_url = get_url_from_req(req)

        # For the purposes of simulating retreiving the raw HESA XML in the absence of
        # the HESA HTTP/REST endpoint, we will simply read from the dummy BLOB container.
        # This will NOT be how the eventual ingestion pipeline works so this will need 
        # to be refactored, either within the Azure Function or using Azure Data Factory's
        # native HTTP connector and COPY DATA functionality.
        # For this dummy function, we will programatically read the HESA XML from the
        # dummy BLOB container using the BLOCKBLOBSERVICE. We could have used an
        # input binding, but since this will be refactored anyway and the production
        # ingestion pipeline will not use an input binding at this stage, we will keep
        # with the programattic approach.

        # Instantiate the Block Blob Service
        blob_service = BlockBlobService(
            account_name = storage_account_name, 
            account_key = storage_account_key)
        #logging.info("Created Block Blob Service to Azure Storage Account '%s'", 
            storage_account_name)

        # Copy the dummy raw HESA XML from the dummy to the landing BLOB container
        # Prefix the output BLOB name with the datetime it is being ingested
        ingest_datetime = datetime.today().strftime('%Y%m%d%H%M%S')
        dummy_raw_hesa_xml_stream = blob_service.copy_blob(
            container_name = output_container_name, 
            blob_name = output_blob_name_prefix + ingest_datetime + '-' + dummy_input_blob_name,
            copy_source = dummy_input_blob_url)
        #logging.info("Ingested raw HESA XML into Azure Storage Account Container '%s'", 
            output_container_name)

        # Return an OK HTTP Response
        return func.HttpResponse(
             "Raw HESA XML successfully ingested into the landing BLOB container.",
             status_code=200)

    except OfsMissingUrlError:
        # Return a Bad Request HTTP Response
        #logging.error(
            "A URL to retrieve the raw XML data from HESA was not found in the POST request", exc_info=True)
        error_data = {
            'errors': [
                {'resource_url': 'The URL to fetch the XML was not provided.'},
            ]
        }
        return func.HttpResponse(json.dumps(error_data), status_code=400)

    except Exception:
        # Return an Internal Server Error HTTP Response
        #logging.error("Raw HESA XML could not be ingested", exc_info=True)
        return func.HttpResponse(
            "Raw HESA XML could not be ingested",
            status_code=500)
