import unittest

from CourseSearchBuilder.build_sitemap_xml import build_course_details_url, build_institution_details_url, build_xml


class TestSiteMapXml(unittest.TestCase):

    def test_course_details_url(self):
        input_english = ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "en")
        result_english = "https://discoveruni.gov.uk/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/"
        input_welsh = ("10008071", "AAUNDERGRADUATE5YEAR", "Full-time", "cy")
        result_welsh = "https://discoveruni.gov.uk/cy/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/"
        self.assertEqual(build_course_details_url(*input_english), result_english)
        self.assertEqual(build_course_details_url(*input_welsh), result_welsh)

    def test_institution_details_url(self):
        input_english = ("10008071", "en")
        result_english = "https://discoveruni.gov.uk/institution-details/10008071/"
        input_welsh = ("10008071", "cy")
        result_welsh = "https://discoveruni.gov.uk/cy/institution-details/10008071/"
        self.assertEqual(build_institution_details_url(*input_english), result_english)
        self.assertEqual(build_institution_details_url(*input_welsh), result_welsh)

    def test_xml_build(self):
        input = [
            "https://discoveruni.gov.uk/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/",
            "https://discoveruni.gov.uk/cy/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/",
            "https://discoveruni.gov.uk/institution-details/10008071/",
            "https://discoveruni.gov.uk/cy/institution-details/10008071/",
        ]
        result = """
        <urlset xmlns="https://discoveruni.gov.uk/sitemap.xml/">
            <url>
                <loc>https://discoveruni.gov.uk/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/)</loc>
                <lastmod>2022-09-04</lastmod>
            </url>
            <url>
                <loc>https://discoveruni.gov.uk/cy/course-details/10008071/AAUNDERGRADUATE5YEAR/Full-time/)</loc>
                <lastmod>2022-09-04</lastmod>
            </url>
            <url>
                <loc>https://discoveruni.gov.uk/institution-details/10008071/)</loc>
                <lastmod>2022-09-04</lastmod>
            </url>
            <url>
                <loc>https://discoveruni.gov.uk/cy/institution-details/10008071/)</loc>
                <lastmod>2022-09-04</lastmod>
            </url>
        </urlset>
        """
        self.assertEqual(build_xml(input), result)
