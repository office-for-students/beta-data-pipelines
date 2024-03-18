import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union


def build_postcode_search_doc(postcode_list: List[str]) -> Union[Dict[str, Any], None]:
    """
    Takes a list containing postcode data and converts to a dictionary to be used for search.
    Returns none if any data is invalid

    :param postcode_list: List containing postcode data
    :type postcode_list: List[str]
    :return: Dictionary containing postcode data for search, or None if data is invalid
    :rtype: Union[Dict[str, Any], None]
    """
    try:
        postcode_object = {
            "postcode": postcode_list[1],
            "latitude": postcode_list[2],
            "longitude": postcode_list[3]
        }

        if postcode_list[2] == "" or postcode_list[3] == "":
            logging.warning(f"Postcode missing coordinates\n \
                              postcode_object: {postcode_object}")
            return None

        is_lat_float, latitude = is_float(postcode_list[2])
        is_long_float, longitude = is_float(postcode_list[3])
        if not is_lat_float or not is_long_float:
            return None

        if validate_latitude(latitude) or validate_longitude(longitude):
            logging.warning(f"coordinates invalid\n\
                              postcode_object:{postcode_object}")
            return None

        postcode = postcode_list[1].lower().replace(' ', '')

        json = {
            "@search.action": "upload",
            "id": postcode_list[0],
            "geo": {
                "type": "Point",
                "coordinates": [longitude, latitude]
            },
            "latitude": latitude,
            "longitude": longitude,
            "postcode": postcode
        }

        return json
    except Exception:
        logging.warning(f"failed to load postcodes\n\
            postcode: {postcode_list[1]}")
        raise


def validate_latitude(latitude: float) -> bool:
    """
    Takes a latitude as an integer and ensures it lies within the UK (so that it's valid for a UK postcode)
    Returns True if the latitude lies outside the UK, otherwise returns False

    :param latitude: Longitude to check
    :type latitude: float
    :return: True if the longitude is outside the UK (invalid), otherwise False (valid)
    :rtype: bool
    """
    if latitude < 49 or latitude > 61:
        logging.warning(f"latitude not valid for a UK postcode\n\
                          latitude:{latitude}")
        return True

    return False


def validate_longitude(longitude: float) -> bool:
    """
    Takes a longitude as an integer and ensures it lies within the UK (so that it's valid for a UK postcode)
    Returns True if the longitude lies outside the UK, otherwise returns False

    :param longitude: Longitude to check
    :type longitude: float
    :return: True if the longitude is outside the UK (invalid), otherwise False (valid)
    :rtype: bool
    """
    if longitude < -11 or longitude > 2.5:
        logging.warning(f"longitude not valid for a UK postcode\n\
                          longitude:{longitude}")
        return True

    return False


def validate_header(header_row: List[str]) -> bool:
    """
    Takes a header row as a list of strings and checks that all required headers are present. Returns False if any
    of the headers are incorrect, otherwise True.

    :param header_row: List of column header strings
    :type header_row: List[str]
    :return: True if the headers are valid, otherwise False
    :rtype: bool
    """
    if header_row[0] != "id":
        return False

    if header_row[1] != "postcode":
        return False

    if header_row[2] != "latitude":
        return False

    if header_row[3] != "longitude":
        return False

    return True


def is_float(value: Any) -> Tuple[bool, Union[float, None]]:
    """
    Checks if the passed value is a float and returns a tuple with the boolean result, and the new float value if
    it is indeed a float.

    :param value: Value to check
    :type value: Any
    :return: Tuple containing the boolean result and new float value if applicable
    :rtype: Tuple[bool, Union[float, None]]
    """
    try:
        new_value = float(value)
        return True, new_value
    except ValueError:
        return False, None
