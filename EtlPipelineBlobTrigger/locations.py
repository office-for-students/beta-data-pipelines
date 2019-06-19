import xmltodict
import xml.etree.ElementTree as ET


class Locations:
    """Provides lookup of raw location data based on UKPRN and LOCID"""

    def __init__(self, root):
        """Build the locations lookup table

        Locations are unique on UKPRN and LOCID
        """

        self.lookup_dict = {}
        for location in root.iter('LOCATION'):
            raw_location_data = xmltodict.parse(
                ET.tostring(location))['LOCATION']
            lockey = f"{raw_location_data['LOCID']}{raw_location_data['UKPRN']}"
            self.lookup_dict[lockey] = raw_location_data

    def get_location_data_for_key(self, key):
        return self.lookup_dict.get(key)
