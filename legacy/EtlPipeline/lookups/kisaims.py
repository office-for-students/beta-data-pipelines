from typing import Dict

import defusedxml.ElementTree as ET
import xmltodict


class KisAims:
    """Provides lookup of raw kisaim label based on KISAIMCODE"""

    def __init__(self, root: ET) -> None:
        """Build the KisAim lookup table

        Locations are unique on KISAIMCODE
        """

        self.lookup_dict = {}
        for kisaim in root.iter("KISAIM"):
            raw_kisaim_data = xmltodict.parse(ET.tostring(kisaim))["KISAIM"]
            key = f"{raw_kisaim_data['KISAIMCODE']}"
            self.lookup_dict[key] = raw_kisaim_data["KISAIMLABEL"]

    def get_kisaim_label_for_key(self, key: str) -> str:
        """
        Takes a key and returns the KIS aim data from the KisAims object's lookup dict.

        :param key: Key to extract KIS aim data with
        :type key: str
        :return: Corresponding KIS aim data
        :rtype: Dict[str, Any]
        """
        return self.lookup_dict.get(key)
