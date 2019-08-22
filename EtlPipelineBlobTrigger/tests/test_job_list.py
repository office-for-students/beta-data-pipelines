import json
import unittest

import xml.etree.ElementTree as ET

import xmltodict

from course_stats import JobList

from testing_utils import get_string


class TestGetStats(unittest.TestCase):
    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        for institution in root.iter("INSTITUTION"):
            raw_inst_data = xmltodict.parse(ET.tostring(institution))[
                "INSTITUTION"
            ]
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))[
                    "KISCOURSE"
                ]
                job_list = JobList()
                json_job_list = job_list.get_stats(raw_course_data)
                if len(json_job_list) > 1:
                    print(json.dumps(json_job_list, indent=4))



# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
