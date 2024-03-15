#!/usr/bin/env python
""" Creates a new DataSet for each new file we get from HESA """

import logging
from typing import Optional

from . import validators
from .dataset_creator import DataSetCreator
from legacy.services.blob import BlobService
from legacy.services.exceptions import StopEtlPipelineErrorException
from legacy.services.exceptions import XmlValidationError


def create_dataset_main(
        blob_service: BlobService,
        storage_container_name: str,
        storage_blob_name: str,
        use_test_xml: bool = False,
        mock_xml: Optional[str] = None
) -> None:
    """
    Creates a new DataSet for each new file we get from HESA

    :param blob_service: Class that contains methods for interacting with Azure Blob Storage
    :param storage_container_name: azure storage container
    :param storage_blob_name: azure storage blob name
    :param use_test_xml: If True will use mock xml file
    :param mock_xml: For testing pass in a mock XML string

    :return: None
    :rtype: None
    """

    if use_test_xml:
        mock_xml_source_file = open(mock_xml, "r")
        xml_string = mock_xml_source_file.read()
    else:
        xml_string = blob_service.get_str_file(storage_container_name, storage_blob_name)

    # BASIC XML Validation
    try:
        validators.parse_xml_data(xml_string)
    except XmlValidationError:
        logging.error("Error unable to parse the XML data from HESA.")
        raise StopEtlPipelineErrorException

    # CREATE NEW DATASET
    data_set_creator = DataSetCreator()
    data_set_creator.load_new_dataset_doc()
