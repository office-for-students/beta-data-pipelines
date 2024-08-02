from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.utils import get_subject_lookups


class SubjectCourseEnricher:
    """Handles enriching courses with UKRLP data"""

    def __init__(self, cosmos_service: type["CosmosService"], version: int) -> None:
        self.subject_lookups = get_subject_lookups(cosmos_service=cosmos_service, version=version)

    def enrich_course(self, course: Dict[str, Any]) -> None:
        """Takes a course and enriches subject object with subject names"""

        subjects = course["course"]["subjects"]
        course["course"]["subjects"] = self.get_subjects(subjects)

    def get_subjects(self, subject_codes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Returns a subject object containing code, english_name, welsh_name and level.
        If a subject is not found, adds the subject code to the list of subjects.

        :param subject_codes: List of subject codes used for lookup
        :type subject_codes: List[Dict[str, Any]]
        :return: List of subject objects
        :rtype: List[Dict[str, Any]]
        """

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
