import unittest
from datetime import datetime

from CourseSearchBuilder.build_sitemap_xml import build_course_details_url, build_institution_details_url, build_xml_string


class TestSiteMapXml(unittest.TestCase):

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
        xml = """<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
        input = [
            ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "en"),
            ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "cy"),
            ("10008071", "en"),
            ("10008071", "cy"),
        ]
        result = f"""<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
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
