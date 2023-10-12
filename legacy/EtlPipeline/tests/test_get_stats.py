# import unittest
#
# import defusedxml.ElementTree as ET
# import xmltodict
#
# import course_stats
# from course_docs import get_country
# from testing_utils import get_string
#
#
# class TestGetStats(unittest.TestCase):
#     def test_with_25_courses(self):
#         """Initial smoke test"""
#         xml_string = get_string("fixtures/25_courses.xml")
#         root = ET.fromstring(xml_string)
#         for institution in root.iter("INSTITUTION"):
#             raw_inst_data = xmltodict.parse(ET.tostring(institution))[
#                 "INSTITUTION"
#             ]
#             country_code = get_country(raw_inst_data)["code"]
#             for course in institution.findall("KISCOURSE"):
#                 raw_course_data = xmltodict.parse(ET.tostring(course))[
#                     "KISCOURSE"
#                 ]
#                 course_stats.get_stats(raw_course_data, country_code)
#
#
# # TODO Test more of the functionality
#
# if __name__ == "__main__":
#     unittest.main()
