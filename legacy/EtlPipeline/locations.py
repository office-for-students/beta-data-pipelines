from typing import Any
from typing import Dict

import xmltodict
import defusedxml.ElementTree as ET


class Locations:
    """Provides lookup of raw location data based on UKPRN and LOCID"""

    def __init__(self, root: ET) -> None:
        """Build the locations lookup table

        Locations are unique on UKPRN and LOCID
        """

        self.lookup_dict = {}
        for location in root.iter("LOCATION"):
            raw_location_data = xmltodict.parse(ET.tostring(location))[
                "LOCATION"
            ]
            lockey = (
                f"{raw_location_data.get('LOCID')}{raw_location_data['UKPRN']}"
            )
            self.lookup_dict[lockey] = raw_location_data

    def get_location_data_for_key(self, key: str) -> Dict[str, Any]:
        """
        Takes a key and returns the location data from the Locations object's lookup dict.

        :param key: Key to extract location data with
        :type key: str
        :return: Corresponding location data
        :rtype: Dict[str, Any]
        """
        return self.lookup_dict.get(key, {})
