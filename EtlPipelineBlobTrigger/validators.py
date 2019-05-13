#!/usr/bin/env python

""" validators.py: ETL Pipeline Validators """

import logging
import os
import xmlschema

from . import exceptions
from distutils.util import strtobool
from xmlschema.validators.exceptions import XMLSchemaValidationError

__author__ = "Jillur Quddus, Nathan Shumoogum"
__credits__ = ["Jillur Quddus", "Nathan Shumoogum"]
__version__ = "0.1"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@methods.co.uk"
__status__ = "Development"

# Get the relevant properties from Application Settings
stop_etl_pipeline_on_warning = bool(strtobool(os.environ['StopEtlPipelineOnWarning']))


def validate_xml(xsd_path, xml_path_or_string) -> bool:

    """ Validate a given XML file or string against its XSD """
    xml_schema = xmlschema.XMLSchema(xsd_path)
    xml_is_valid = xml_schema.is_valid(xml_path_or_string)
    
    # If the xml is not valid then log the specific XML schema validation error
    if xml_is_valid:
        logging.info("XML is valid")
    
    elif not xml_is_valid:
        try:
            xml_schema.validate(xml_path_or_string)
        except XMLSchemaValidationError:
            logging.warn("XML is not valid", exc_info=True)
            if stop_etl_pipeline_on_warning:
                raise exceptions.StopEtlPipelineWarningException
    
    return xml_is_valid
