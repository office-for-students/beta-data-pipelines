import logging

import defusedxml.ElementTree as ET

from legacy.services.exceptions import XmlValidationError


def parse_xml_data(xml_string) -> None:
    """ Basic check that we can parse the received XML"""

    try:
        ET.fromstring(xml_string)
    except ET.ParseError as e:
        logging.error("Error unable to parse the XML.")
        raise XmlValidationError
