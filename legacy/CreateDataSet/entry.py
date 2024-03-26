#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import logging
from typing import Optional

from . import validators
from .dataset_creator import DataSetCreator
from legacy.services.blob import BlobService
from legacy.services.exceptions import StopEtlPipelineErrorException
from legacy.services.exceptions import XmlValidationError
from ..services.cosmosservice import CosmosService


def create_dataset_main(
        blob_service: BlobService,
        cosmos_service: CosmosService,
        storage_container_name: str,
        storage_blob_name: str
) -> None:
    """
    Creates a new DataSet for each new file we get from HESA

    :param blob_service: Class that contains methods for interacting with Azure Blob Storage
    :type blob_service: BlobService
    :param cosmos_service: Cosmos database service used to create dataset creator object
    :type cosmos_service: CosmosService
    :param storage_container_name: Azure storage container
    :type storage_container_name: str
    :param storage_blob_name: Azure storage blob name
    :type storage_container_name: str

    :return: None
    """

    xml_string = blob_service.get_str_file(storage_container_name, storage_blob_name)

    # BASIC XML Validation
    try:
        validators.parse_xml_data(xml_string)
    except XmlValidationError:
        logging.error("Error unable to parse the XML data from HESA.")
        raise StopEtlPipelineErrorException

    # CREATE NEW DATASET
    data_set_creator = DataSetCreator(cosmos_service=cosmos_service)
    data_set_creator.load_new_dataset_doc()
