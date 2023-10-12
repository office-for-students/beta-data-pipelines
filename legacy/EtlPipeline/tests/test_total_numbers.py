"""Test the course NSS statistics"""
import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from EtlPipeline.course_stats import Continuation
from EtlPipeline.course_stats import Employment
from EtlPipeline.course_stats import Entry
from EtlPipeline.course_stats import JobType
from EtlPipeline.course_stats import Nss
from EtlPipeline.course_stats import Tariff
from EtlPipeline.course_stats import get_stats
from EtlPipeline.tests.test_helpers.testing_utils import get_string

class TestTotalNumbers(unittest.TestCase):
    def setUp(self) -> None:
        self.target = 33474
        self.xml_string = get_string("fixtures/test_questions.xml")
        self.xml_root = ET.fromstring(self.xml_string)

    def test_total_nss(self):
        total_items = list()
        nss = Nss()
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(nss.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_entry(self):
        total_items = list()
        entry = Entry()
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(entry.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_continuation(self):
        total_items = list()
        continuation = Continuation()
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(continuation.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_employment(self):
        total_items = list()
        employment = Employment()
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(employment.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_job_type(self):
        total_items = list()
        job_type = JobType()
        for institution in self.xml_root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                total_items.append(job_type.get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

    def test_total_tariff(self):
        total_items = list()
        tariff = Tariff()
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
                total_items.append(get_stats(raw_course_data))

        self.assertEqual(len(total_items), self.target)

if __name__ == "__main__":
    unittest.main()
