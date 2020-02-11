import logging

from __app__.SharedCode import utils


class SubjectCourseEnricher:
    """Handles enriching courses with UKRLP data"""

    def __init__(self, version):
        self.subject_lookups = utils.get_subject_lookups(version)

    def enrich_course(self, course):
        """Takes a course and enriches subject object with subject names"""

        subjects = course["course"]["subjects"]
        course["course"]["subjects"] = self.get_subjects(subjects)

    def get_subjects(self, subject_codes):
        """Returns a subject object containing code, english_name, welsh_name and level"""

        subjects = []
        for subject in subject_codes:

            code = subject["code"]
            if code not in self.subject_lookups:
                subjects.append(subject)
                continue

            level = self.subject_lookups[code].get("level", "")
            english = self.subject_lookups[code].get("english_name", "")
            welsh = self.subject_lookups[code].get("welsh_name", "")

            if level != "":
                subject["level"] = level

            if english != "":
                subject["english"] = english

            if welsh != "":
                subject["welsh"] = welsh

            subjects.append(subject)

        return subjects
