import xmltodict
import xml.etree.ElementTree as ET

class Locations:
    """Provides lookup of location data based on location id"""


    def __init__(self, root):
        """Build the locations lookup table"""

        self.lookup_dict = {}
        for location in root.iter('LOCATION'):
            raw_location_data = xmltodict.parse(ET.tostring(location))['LOCATION']
            locid = raw_location_data['LOCID']
            self.lookup_dict[locid] = raw_location_data

    def get_location_data_for_locid(self, locid):
        """Return the location data for a locid or None if no data available"""
        return self.lookup_dict.get(locid)
