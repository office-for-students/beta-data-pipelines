import logging


def build_postcode_search_doc(postcode_list):
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


def validate_latitude(latitude):
    if latitude < 49 or latitude > 61:
        logging.warning(f"latitude not valid for a UK postcode\n\
                          latitude:{latitude}")
        return True

    return False


def validate_longitude(longitude):
    if longitude < -11 or longitude > 2.5:
        logging.warning(f"longitude not valid for a UK postcode\n\
                          longitude:{longitude}")
        return True

    return False


def validate_header(header_row):
    invalid = False
    if header_row[0] != "id":
        invalid = True

    if header_row[1] != "postcode":
        invalid = True

    if header_row[2] != "latitude":
        invalid = True

    if header_row[3] != "longitude":
        invalid = True

    return invalid


def is_float(value):
    try:
        new_value = float(value)
        return True, new_value
    except ValueError:
        return False, None
