import xmltodict
import defusedxml.ElementTree as ET


class SectorSalaries:
    """Provides lookup of raw sector salary data based on GO- LEO3- LEO5- -SECSBJ"""

    def __init__(self, root, sector_type):
        """Build the sector salaries lookup table

        Sector salaries are unique on -SECSBJ
        """

        self.lookup_dict = {}

        extension = 'SECSAL' if sector_type == "GO" else 'SEC'

        sector_salary_type = sector_type + extension
        sector_salary_key_type = sector_type + 'SECSBJ'

        sector_sal_root = root.find('SECTORSAL')
        for sector_salary in sector_sal_root.iter(sector_salary_type):
            raw_sector_salary_data = xmltodict.parse(
                ET.tostring(sector_salary)
            )[sector_salary_type]

            sector_salary_key = f"{raw_sector_salary_data[sector_salary_key_type]}"
            self.lookup_dict[sector_salary_key] = raw_sector_salary_data

    def get_sector_salaries_data_for_key(self, key):
        return self.lookup_dict.get(key)


class GOSectorSalaries(SectorSalaries):

    def __init__(self, root):
        self.geo_sector_salary_type = 'GO'
        super().__init__(root, self.geo_sector_salary_type)


class LEO3SectorSalaries(SectorSalaries):

    def __init__(self, root):
        self.leo3_sector_salary_type = 'LEO3'
        super().__init__(root, self.leo3_sector_salary_type)


class LEO5SectorSalaries(SectorSalaries):

    def __init__(self, root):
        self.leo5_sector_salary_type = 'LEO5'
        super().__init__(root, self.leo5_sector_salary_type)
