import xmltodict
import defusedxml.ElementTree as ET


class SalarySector:
    # Extracts the sector-level <SECTORSAL/xxxSECSAL> node appropriate to the current course

    def __init__(self, root, raw_course_data, salary_inst_array, salary_node_name):
        self.matching_sector_array =[]
        sector_sal_root = root.find('SECTORSAL')
        course_mode = raw_course_data["KISMODE"]
        course_level = raw_course_data["KISLEVEL"]

        for sector in sector_sal_root.iter(salary_node_name):
            raw_sector_data = xmltodict.parse(
                ET.tostring(sector)
            )[salary_node_name]

            sector_sbj = raw_sector_data["SBJ"]
            sector_mode = raw_sector_data["KISMODE"]
            sector_level = raw_sector_data["KISLEVEL"]

            for salary_inst in salary_inst_array:
                course_sbj = salary_inst["sbj"]

                if sector_sbj == course_sbj and sector_mode == course_mode and sector_level == course_level:
                    self.matching_sector_array.append(raw_sector_data)
                    break

            if len(self.matching_sector_array) == len(salary_inst_array):
                break


    def get_matching_sector_array(self):
        return self.matching_sector_array