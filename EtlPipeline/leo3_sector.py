import xmltodict
import defusedxml.ElementTree as ET


class Leo3Sector:
    """Extracts the sector-level <LEO3> node appropriate to the current course"""

    def __init__(self, root, raw_course_data):
        self.matching_sector = None

        for sector in root.iter("LEO3"):
            raw_sector_data = xmltodict.parse(
                ET.tostring(sector)
            )["LEO3"]

            sector_sbj = raw_sector_data["SBJ"]
            sector_mode = raw_sector_data["MODE"]
            sector_level = raw_sector_data["LEVEL"]
            course_sbj = raw_course_data["LEO3"]["LEO3SBJ"]
            course_mode = raw_course_data["KISMODE"]
            course_level = raw_course_data["KISLEVEL"]

            if sector_sbj == course_sbj and sector_mode == course_mode and sector_level == course_level:
                self.matching_sector = raw_sector_data
                break


    def get_matching_sector(self):
        return self.matching_sector