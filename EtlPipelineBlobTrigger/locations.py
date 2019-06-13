import xmltodict
import xml.etree.ElementTree as ET

class Locations:
    """Provides lookup of location data based on location id"""


    def __init__(self, root):
        """Build the locations lookup table

        Locations are unique on UKPRN and LOCID
        """

        used_location_keys = []

        self.lookup_dict = {}
        for location in root.iter('LOCATION'):
            raw_location_data = xmltodict.parse(ET.tostring(location))['LOCATION']
            lockey = f"{raw_location_data['LOCID']}{raw_location_data['UKPRN']}"
            if lockey in used_location_keys:
                # Each location key should be unique according to schema
                print("Key not unique !!!!")
                sys.exit()
            used_location_keys.append(lockey)
            self.lookup_dict[lockey] = raw_location_data

    def get_location_data_for_locid(self, locid):
        """Return the location data for a locid or None if no data available"""
        return self.lookup_dict.get(locid)
