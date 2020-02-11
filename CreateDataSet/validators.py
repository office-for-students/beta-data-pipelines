import logging

import defusedxml.ElementTree as ET

from __app__.SharedCode.exceptions import XmlValidationError


def parse_xml_data(xml_string):
    """ Basic check that we can parse the received XML"""

    try:
        ET.fromstring(xml_string)
    except ET.ParseError as e:
        logging.error("Error unable to parse the XML.")
        raise XmlValidationError
