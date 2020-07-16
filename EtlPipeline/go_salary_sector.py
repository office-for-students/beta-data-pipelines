import xmltodict
import defusedxml.ElementTree as ET


class GoSalarySector:
    """Extracts the sector-level <GOSALARY> node appropriate to the current course"""

    def __init__(self, root, raw_course_data):
        self.matching_sector = None

        for sector in root.iter("GOSALARY"):
            raw_sector_data = xmltodict.parse(
                ET.tostring(sector)
            )["GOSALARY"]

            sector_sbj = raw_sector_data["SBJ"]
            sector_mode = raw_sector_data["MODE"]
            sector_level = raw_sector_data["LEVEL"]
            course_sbj = raw_course_data["GOSALARY"]["SALSBJ"]
            course_mode = raw_course_data["KISMODE"]
            course_level = raw_course_data["KISLEVEL"]

            if sector_sbj == course_sbj and sector_mode == course_mode and sector_level == course_level:
                self.matching_sector = raw_sector_data
                break


    def get_matching_sector(self):
        return self.matching_sector