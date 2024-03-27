#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import logging
from typing import Type

from services.blob_service.base import BlobServiceBase
from services.dataset_service import DataSetService
from services.exceptions import StopEtlPipelineErrorException
from services.exceptions import XmlValidationError
from . import validators
from .dataset_creator import DataSetCreator


def create_dataset_main(
        blob_service: Type["BlobServiceBase"],
        dataset_service: DataSetService,
        storage_container_name: str,
        storage_blob_name: str
) -> None:
    """
    Creates a new DataSet for each new file we get from HESA

    :param blob_service: Class that contains methods for interacting with Azure Blob Storage
    :type blob_service: BlobService
    :param dataset_service: Dataset service used to create dataset creator object
    :type dataset_service: DataSetService
    :param storage_container_name: Azure storage container
    :type storage_container_name: str
    :param storage_blob_name: Azure storage blob name
    :type storage_container_name: str

    :return: None
    """

    xml_string = blob_service.get_str_file(
        container_name=storage_container_name,
        blob_name=storage_blob_name
    )

    # BASIC XML Validation
    try:
        validators.parse_xml_data(xml_string)
    except XmlValidationError:
        logging.error("Error unable to parse the XML data from HESA.")
        raise StopEtlPipelineErrorException

    # CREATE NEW DATASET
    data_set_creator = DataSetCreator(
        dataset_service=dataset_service
    )
    data_set_creator.load_new_dataset_doc()
