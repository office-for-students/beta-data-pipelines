import logging
import jsonschema


def check_valid_xml_data(xml_string):
    """ Basic check that we can parse the received XML"""

    try:
        ET.fromstring(xml_string)
    except ET.ParseError as e:
        logging.error("Error unable to parse the XML.")
        raise e
