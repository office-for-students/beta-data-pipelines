import logging

import defusedxml.ElementTree as ET

from legacy.services.exceptions import XmlValidationError


def parse_xml_data(xml_string: str) -> None:
    """
    Basic check that we can parse the received XML

    :param xml_string: XML string to check
    :type xml_string: str
    :return: None
    """

    try:
        ET.fromstring(xml_string)
    except ET.ParseError as e:
        logging.error("Error unable to parse the XML.")
        raise XmlValidationError
