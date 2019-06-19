import xmltodict
import xml.etree.ElementTree as ET


class KisAims:
    """Provides lookup of raw kisaim label based on KISAIMCODE"""

    def __init__(self, root):
        """Build the KisAim lookup table

        Locations are unique on KISAIMCODE
        """

        self.lookup_dict = {}
        for kisaim in root.iter('KISAIM'):
            raw_kisaim_data = xmltodict.parse(ET.tostring(kisaim))['KISAIM']
            key = f"{raw_kisaim_data['KISAIMCODE']}"
            self.lookup_dict[key] = raw_kisaim_data['KISAIMLABEL']

    def get_kisaim_label_for_key(self, key):
        return self.lookup_dict.get(key)
