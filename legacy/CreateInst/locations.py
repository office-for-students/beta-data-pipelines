from typing import Any

import defusedxml.ElementTree as ET
import xmltodict


class Locations:
    """Provides lookup of location data based on UKPRN and LOCID"""

    def __init__(self, root: ET) -> None:
        """Build the locations lookup table

        The key is comprised of two parts:

            UKPRN
            LOCID

        If LOCATION.LOCUKPRN is present, we use that for the UKPRN part
        otherwise we use LOCATION.UKPRN for the UKPRN part.

        The LOCID part is the LOCATION.LOCID

        """

        self.lookup_dict = {}
        for location in root.iter("LOCATION"):
            raw_location_data = xmltodict.parse(ET.tostring(location))["LOCATION"]
            ukprn = raw_location_data["UKPRN"]
            if "LOCUKPRN" in raw_location_data:
                ukprn = raw_location_data["LOCUKPRN"]
            lockey = f"{ukprn}{raw_location_data['LOCID']}"
            self.lookup_dict[lockey] = raw_location_data

    def get_location(self, key: str) -> Any:
        """
        Takes a key and returns the corresponding value from the lookup dictionary, defaults to returning an
        empty dictionary.

        :param key: Key to lookup with
        :return: Value of the lookup dictionary with the passed key
        :rtype: Any
        """
        return self.lookup_dict.get(key, {})
