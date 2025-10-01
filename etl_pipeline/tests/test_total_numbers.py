"""Test the course NSS statistics"""
import unittest
from unittest.mock import MagicMock

import defusedxml.ElementTree as ET
import xmltodict

from etl_pipeline.course_stats import Continuation
from etl_pipeline.course_stats import Employment
from etl_pipeline.course_stats import Entry
from etl_pipeline.course_stats import JobType
from etl_pipeline.course_stats import Nss
from etl_pipeline.course_stats import Tariff
from etl_pipeline.course_stats import get_stats
from etl_pipeline.tests.test_helpers.testing_utils import get_string


class TestTotalNumbers(unittest.TestCase):
    def setUp(self) -> None:
        self.target = 467
        self.xml_string = get_string("fixtures/test_questions.xml")
        self.xml_root = ET.fromstring(self.xml_string)

    def test_total_nss(self):
        total_items = list()
        nss = Nss(subject_codes={})
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(nss.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_entry(self):
        total_items = list()
        entry = Entry(subject_codes={})
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(entry.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_continuation(self):
        total_items = list()
        continuation = Continuation(subject_codes={})
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(continuation.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_employment(self):
        total_items = list()
        employment = Employment(subject_codes={})
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(employment.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_job_type(self):
        total_items = list()
        job_type = JobType(subject_codes={})
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(job_type.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_tariff(self):
        total_items = list()
        tariff = Tariff(subject_codes={})
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(tariff.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_absolute_total(self):
        total_items = list()
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(get_stats(raw_course_data, dict()))

        self.assertEqual(len(total_items), self.target)


if __name__ == "__main__":
    unittest.main()
