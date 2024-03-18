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
            try:
                raw_location_data = xmltodict.parse(ET.tostring(location))[
                    "LOCATION"
                ]
                ukprn = raw_location_data["UKPRN"]
                if "LOCUKPRN" in raw_location_data:
                    ukprn = raw_location_data["LOCUKPRN"]
                lockey = f"{ukprn}{raw_location_data['LOCID']}"
                self.lookup_dict[lockey] = raw_location_data
            except Exception as e:
                if hasattr(e, 'message'):
                    exception_text = e.message
                else:
                    exception_text = e

                # f = open("locations_debug.txt", "a")
                # if location: f.write(f"location: {location}\n")
                # if ukprn: f.write(f"ukprn: {ukprn}\n")
                # if lockey: f.write(f"lockey: {lockey}\n")
                # if raw_location_data: f.write(f"raw_location_data: {raw_location_data}\n")
                # if self.lookup_dict: f.write(f"lookup_dict: {self.lookup_dict}\n")
                # f.write(f"exception_text: {exception_text}\n")
                # f.write("======================================================================================\n")
                # f.write("======================================================================================\n")
                # f.close()

    def get_location(self, key: str) -> Any:
        """
        Takes a key and returns the corresponding value from the lookup dictionary, defaults to returning an
        empty dictionary.

        :param key: Key to lookup with
        :return: Value of the lookup dictionary with the passed key
        :rtype: Any
        """
        return self.lookup_dict.get(key, {})
