import xml.etree.ElementTree as ET
import xmltodict


class Locations:
    """Provides lookup of location data based on UKPRN and LOCID"""

    def __init__(self, root):
        """Build the locations lookup table

        The key is comprised of two parts:

            UKPRN
            LOCID

        If LOCATION.LOCUKPRN is present, we use that for the UKPRN part
        otherwise we use LOCATION.UKPRN for the UKPRN part.

        The LOCID part is the LOCATION.LOCID

        We are not sure if this mapping is correct; it may need a
        modification after consulting with OfS.
        """

        self.lookup_dict = {}
        for location in root.iter('LOCATION'):
            raw_location_data = xmltodict.parse(
                ET.tostring(location))['LOCATION']
            ukprn = raw_location_data['UKPRN']
            if 'LOCUKPRN' in raw_location_data:
                ukprn = raw_location_data['LOCUKPRN']
            lockey = f"{ukprn}{raw_location_data['LOCID']}"
            self.lookup_dict[lockey] = raw_location_data

    def get_location_data_for_key(self, key):
        return self.lookup_dict.get(key)
