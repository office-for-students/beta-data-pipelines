import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict
from unittest import mock

from legacy.EtlPipeline.course_docs import get_go_inst_json
from legacy.EtlPipeline.course_docs import get_go_sector_json
from legacy.EtlPipeline.sector_salaries import GOSectorSalaries
from legacy.EtlPipeline.sector_salaries import LEO3SectorSalaries
from legacy.EtlPipeline.sector_salaries import LEO5SectorSalaries
from legacy.EtlPipeline.tests.test_helpers.testing_utils import get_string


class TestProcessData(unittest.TestCase):
    def setUp(self) -> None:
        self.xml_string = get_string("fixtures/latest.xml")
        self.xml_root = ET.fromstring(self.xml_string)

    @mock.patch("EtlPipeline.subject_enricher.SubjectCourseEnricher")
    def test_get_go_inst_no_data(self, subject_enricher):

        for course in self.xml_root.iter("KISCOURSE"):
            raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
            mode = raw_course_data["KISMODE"]
            level = raw_course_data["KISLEVEL"]
            go_salary_inst = get_go_inst_json(raw_course_data["GOSALARY"], subject_enricher=subject_enricher)
            leo3_salary_inst = get_go_inst_json(raw_course_data["LEO3"], subject_enricher=subject_enricher)
            leo5_salary_inst = get_go_inst_json(raw_course_data["LEO5"], subject_enricher=subject_enricher)
            go_sector_salaries = GOSectorSalaries(self.xml_root)
            leo3_sector_salaries = LEO3SectorSalaries(self.xml_root)
            leo5_sector_salaries = LEO5SectorSalaries(self.xml_root)
            json_obj = get_go_sector_json(go_salary_inst, leo3_salary_inst, leo5_salary_inst, go_sector_salaries, mode, level, subject_enricher)
