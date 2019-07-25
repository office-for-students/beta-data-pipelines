import xmltodict
import xml.etree.ElementTree as ET


class Accreditations:
    """Provides lookup of raw accreditation data based on ACCTYPE"""

    def __init__(self, root):
        """Build the accreditations lookup table

        Accreditations are unique on ACCTYPE
        """

        self.lookup_dict = {}
        for accreditation in root.iter('ACCREDITATIONTABLE'):
            raw_accreditation_data = xmltodict.parse(
                ET.tostring(accreditation))['ACCREDITATIONTABLE']

            acckey = f"{raw_accreditation_data['ACCTYPE']}"
            self.lookup_dict[acckey] = raw_accreditation_data

    def get_accreditation_data_for_key(self, key):
        return self.lookup_dict.get(key)
