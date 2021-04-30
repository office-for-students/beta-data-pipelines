import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from accreditations import Accreditations
from course_docs import get_course_doc, get_locids
from kisaims import KisAims
from locations import Locations
from sector_salaries import GOSectorSalaries, LEO3SectorSalaries, LEO5SectorSalaries
from testing_utils import get_first, get_string, remove_variable_elements


INSTITUTION = "INSTITUTION"
UKPRN = "UKPRN"
KISCOURSE = "KISCOURSE"
VERSION = 63

class TestCourseDoc(unittest.TestCase):

    def test_missing_sbj(self):
        # ARRANGE
        xml_string = get_string("fixtures/course_missing_sbj.xml")
        root = ET.fromstring(xml_string)

        accreditations = Accreditations(root)
        locations = Locations(root)
        kisaims = KisAims(root)
        go_sector_salaries = GOSectorSalaries(root)
        leo3_sector_salaries = LEO3SectorSalaries(root)
        leo5_sector_salaries = LEO5SectorSalaries(root)
        
        institution = get_institution(root)
        course = get_course(institution)
        raw_course_data = get_raw_course_data(course)
        raw_inst_data = get_raw_inst_data(institution)
        ukprn = get_ukprn(raw_inst_data)
        locids = get_locids(raw_course_data, ukprn)

        # expected_course_doc = json.loads(
        #     get_string("fixtures/one_inst_one_course_with_crsecsturl.json")
        # )

        # ACT
        course_doc = get_course_doc(
            accreditations,
            locations,
            locids,
            raw_inst_data,
            raw_course_data,
            kisaims,
            VERSION,
            go_sector_salaries,
            leo3_sector_salaries,
            leo5_sector_salaries,
        )
        course_doc = remove_variable_elements(course_doc)

        # # ASSERT
        # self.assertEqual(expected_course_doc, course_doc)


def get_institution(root):
    return get_first(root.iter(INSTITUTION))

def get_course(institution):
    return get_first(institution.findall(KISCOURSE))

def get_raw_course_data(course):
    return xmltodict.parse(ET.tostring(course))[KISCOURSE]

def get_raw_inst_data(institution):
    return xmltodict.parse(ET.tostring(institution))[INSTITUTION]

def get_ukprn(raw_inst_data):
    return raw_inst_data[UKPRN]

if __name__ == "__main__":
    unittest.main()
