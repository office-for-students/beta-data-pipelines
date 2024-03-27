#!/usr/bin/env python
""" Validators for the ETL pipeline

    We don't currently throw an exception if we encounter an error, rather we
    let it bubble up to provide more context.

"""

import logging
from distutils.util import strtobool
from typing import Any
from typing import Dict

import xmlschema
from xmlschema.validators.exceptions import XMLSchemaValidationError

__author__ = "Jillur Quddus, Nathan Shumoogum"
__credits__ = ["Jillur Quddus", "Nathan Shumoogum"]
__version__ = "0.1"

from constants import STOP_ETL_PIPELINE_ON_WARNING
from services import exceptions

_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@methods.co.uk"
__status__ = "Development"


def validate_xml(xsd_path: str, xml_path_or_string: str) -> bool:
    """
    Validate a given XML file or string against its XSD.

    :param xsd_path: XSD file path to be validated against
    :type xsd_path: str
    :param xml_path_or_string: XML file path or string
    :type xml_path_or_string: str
    :return: True if the XML is valid, otherwise False
    :rtype: bool
    """
    stop_etl_pipeline_on_warning = bool(strtobool(STOP_ETL_PIPELINE_ON_WARNING))
    xml_schema = xmlschema.XMLSchema(xsd_path)
    xml_is_valid = xml_schema.is_valid(xml_path_or_string)

    # If the xml is not valid then log the specific XML schema validation error
    if xml_is_valid:
        logging.info("XML is valid")
        return True

    try:
        xml_schema.validate(xml_path_or_string)
    except XMLSchemaValidationError:
        logging.warning("XML is not valid", exc_info=True)
        if stop_etl_pipeline_on_warning:
            raise exceptions.StopEtlPipelineWarningException
    return xml_is_valid


def validate_unavailable_reason_code(unavail_reason_code: str) -> None:
    """
    Check the code read from the ????UNAVAILREASON is valid.

    :param unavail_reason_code: Unavailable reason code to be checked
    :type unavail_reason_code: str
    :return: None
    """

    valid_codes = ["0", "1", "2"]
    if unavail_reason_code not in valid_codes:
        logging.error(
            f"The unavailable reason code is invalid {unavail_reason_code}",
            exc_info=True,
        )
        raise exceptions.StopEtlPipelineErrorException


def validate_leo_unavailable_reason_code(unavail_reason_code: str) -> None:
    """
    Check the code read from LEOUNAVAILREASON is valid for no data

    :param unavail_reason_code: Unavailable reason code to be checked
    :type unavail_reason_code: str
    :return: None
    """

    valid_codes = ["1", "2"]
    if unavail_reason_code not in valid_codes:
        logging.error(
            f"The unavailable reason code is invalid {unavail_reason_code}",
            exc_info=True,
        )
        raise exceptions.StopEtlPipelineErrorException


def validate_leo_element_with_data(xml_elem: Dict[str, Any], country_code: str) -> None:
    """
    Check the country code is XF if we have data

    :param xml_elem: XML element containing LEO data
    :type xml_elem: Dict[str, Any]
    :param country_code: Country code to be checked
    :type country_code: str
    :return: None
    """
    if country_code != "XF":
        logging.error(
            f"Unexpected country_code {country_code} with LEO data {xml_elem}",
            exc_info=True,
        )
        raise exceptions.StopEtlPipelineErrorException


def validate_agg(unavail_reason_code: str, agg: str, lookup: Dict[str, Any]) -> None:
    """
    Check the agg code is valid for an unavailable string.

    :param unavail_reason_code: Lookup code for unavailable reason
    :type unavail_reason_code: str
    :param agg: Aggregation code for unavailable reason
    :type agg: str
    :param lookup: Lookup dictionary containing unavailable reason and aggregation data
    :type lookup: Dict[str, Any]
    :return: None
    """

    # The valid values for agg depends on if there is data available and the
    # unavail reason code.

    validate_unavailable_reason_code(unavail_reason_code)

    try:
        lookup["data"][unavail_reason_code][agg]
    except KeyError:
        logging.error(f"The aggregation value is invalid {agg}", exc_info=True)
        raise exceptions.StopEtlPipelineErrorException
