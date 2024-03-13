import unittest
from datetime import datetime

from legacy.CourseSearchBuilder.build_sitemap_xml import build_course_details_url
from legacy.CourseSearchBuilder.build_sitemap_xml import build_institution_details_url
from legacy.CourseSearchBuilder.build_sitemap_xml import build_param_lists
from legacy.CourseSearchBuilder.build_sitemap_xml import build_xml_string
from legacy.CourseSearchBuilder.build_sitemap_xml import get_course_params
from legacy.CourseSearchBuilder.build_sitemap_xml import get_institution_params
from test_course_search_models import get_json


class TestSiteMapXml(unittest.TestCase):

    def test_build_param_lists(self):
        institutions = get_json("fixtures/institution_list.json")
        courses = get_json("fixtures/course_list.json")
        institutions_params, courses_params = build_param_lists(institutions, courses)
        institutions_result = [("10000047", "en"), ("10000047", "cy")]
        courses_result = [("10000055", "AB20", "Full-time", "en"), ("10000055", "AB20", "Full-time", "cy")]
        self.assertEqual(institutions_params, institutions_result)
        self.assertEqual(courses_params, courses_result)

    def test_get_institution_params(self):
        institutions = get_json("fixtures/institution_list.json")
        institutions_params = get_institution_params(institutions)
        institutions_result = [("10000047", "en"), ("10000047", "cy")]
        self.assertEqual(institutions_params, institutions_result)

    def test_get_course_params(self):
        courses = get_json("fixtures/course_list.json")
        courses_params = get_course_params(courses)
        courses_result = [("10000055", "AB20", "Full-time", "en"), ("10000055", "AB20", "Full-time", "cy")]
        self.assertEqual(courses_params, courses_result)

    def test_course_details_url(self):
        input_english = ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "en")
        result_english = "https://discoveruni.gov.uk/en/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/"
        input_welsh = ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "cy")
        result_welsh = "https://discoveruni.gov.uk/cy/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/"
        self.assertEqual(build_course_details_url(*input_english), result_english)
        self.assertEqual(build_course_details_url(*input_welsh), result_welsh)

    def test_institution_details_url(self):
        input_english = ("10008071", "en")
        result_english = "https://discoveruni.gov.uk/en/institution-details/10008071/"
        input_welsh = ("10008071", "cy")
        result_welsh = "https://discoveruni.gov.uk/cy/institution-details/10008071/"
        self.assertEqual(build_institution_details_url(*input_english), result_english)
        self.assertEqual(build_institution_details_url(*input_welsh), result_welsh)

    def test_xml_build(self):
        today = datetime.strftime(datetime.today(), "%Y-%m-%d")
        xml = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
        input = [
            ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "en"),
            ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "cy"),
            ("10008071", "en"),
            ("10008071", "cy"),
        ]
        result = f"""
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://discoveruni.gov.uk/en/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/></loc>
                <lastmod>{today}</lastmod>
            </url>
            <url>
                <loc>https://discoveruni.gov.uk/cy/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/></loc>
                <lastmod>{today}</lastmod>
            </url>
            <url>
                <loc>https://discoveruni.gov.uk/en/institution-details/10008071/></loc>
                <lastmod>{today}</lastmod>
            </url>
            <url>
                <loc>https://discoveruni.gov.uk/cy/institution-details/10008071/></loc>
                <lastmod>{today}</lastmod>
            </url>
        </urlset>"""
        self.assertEqual(build_xml_string(input, xml), result, "XML is not equal")
