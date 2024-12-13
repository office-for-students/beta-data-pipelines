#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import logging
import traceback
from typing import Any

from services.exceptions import StopEtlPipelineErrorException
from services.exceptions import XmlValidationError
from . import validators
from .dataset_creator import DataSetCreator


def create_dataset_main(
        blob_service: type['BlobServiceBase'],
        dataset_service: type['DataSetServiceBase'],
        storage_container_name: str,
        storage_blob_name: str
) -> dict[str, Any]:
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

    response = {}
    try:
        xml_string = blob_service.get_str_file(
            container_name=storage_container_name,
            blob_name=storage_blob_name
        )

        # BASIC XML Validation
        try:
            validators.parse_xml_data(xml_string)
        except XmlValidationError as e:
            logging.error("Error unable to parse the XML data from HESA.")
            raise StopEtlPipelineErrorException

        # CREATE NEW DATASET
        data_set_creator = DataSetCreator(
            dataset_service=dataset_service
        )
        data_set_creator.load_new_dataset_doc()

        response["message"] = "dataset_creation finished successfully"
        response["statusCode"] = 200

    except Exception as e:
        response["message"] = "dataset_creation failed"
        response["exception"] = traceback.format_exc()
        response["statusCode"] = 500

    return response
