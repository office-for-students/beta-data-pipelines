#!/usr/bin/env python
""" Validators for the ETL pipeline

    We don't currently throw an exception if we encounter an error, rather we
    let it bubble up to provide more context.

"""

#import logging
import os
from distutils.util import strtobool

import xmlschema
from xmlschema.validators.exceptions import XMLSchemaValidationError

from SharedCode import exceptions

__author__ = "Jillur Quddus, Nathan Shumoogum"
__credits__ = ["Jillur Quddus", "Nathan Shumoogum"]
__version__ = "0.1"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@methods.co.uk"
__status__ = "Development"


def validate_xml(xsd_path, xml_path_or_string) -> bool:
    """ Validate a given XML file or string against its XSD """
    stop_etl_pipeline_on_warning = bool(
        strtobool(os.environ["StopEtlPipelineOnWarning"])
    )
    xml_schema = xmlschema.XMLSchema(xsd_path)
    xml_is_valid = xml_schema.is_valid(xml_path_or_string)

    # If the xml is not valid then log the specific XML schema validation error
    if xml_is_valid:
        #logging.info("XML is valid")

    elif not xml_is_valid:
        try:
            xml_schema.validate(xml_path_or_string)
        except XMLSchemaValidationError:
            #logging.warning("XML is not valid", exc_info=True)
            if stop_etl_pipeline_on_warning:
                raise exceptions.StopEtlPipelineWarningException

    return xml_is_valid


def validate_unavailable_reason_code(unavail_reason_code):
    """Check the code read from the ????UNAVAILREASON is valid"""

    valid_codes = ["0", "1", "2"]
    if unavail_reason_code not in valid_codes:
        #logging.error(
        #    f"The unavailable reason code is invalid {unavail_reason_code}",
        #    exc_info=True,
        #)
        raise exceptions.StopEtlPipelineErrorException


def validate_leo_unavailable_reason_code(unavail_reason_code):
    """Check the code read from LEOUNAVAILREASON is valid for no data"""

    valid_codes = ["1", "2"]
    if unavail_reason_code not in valid_codes:
        #logging.error(
        #    f"The unavailable reason code is invalid {unavail_reason_code}",
        #    exc_info=True,
        #)
        raise exceptions.StopEtlPipelineErrorException


def validate_leo_element_with_data(xml_elem, country_code):
    """Check the country code is XF is we have data"""
    if country_code != "XF":
        #logging.error(
        #    f"Unexpected country_code {country_code} with LEO data {xml_elem}",
        #    exc_info=True,
        #)
        raise exceptions.StopEtlPipelineErrorException


def validate_agg(unavail_reason_code, agg, lookup):
    """Check the agg code is valid for an unavailable string"""

    # The valid values for agg depends on if there is data available and the
    # unavail reason code.

    validate_unavailable_reason_code(unavail_reason_code)

    try:
        lookup["data"][unavail_reason_code][agg]
    except KeyError:
        #logging.error(f"The aggregation value is invalid {agg}", exc_info=True)
        raise exceptions.StopEtlPipelineErrorException
