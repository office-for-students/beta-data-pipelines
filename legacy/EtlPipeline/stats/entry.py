from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Entry:
    """Extracts and transforms the Entry course element"""

    def __init__(self, subject_codes) -> None:
        self.shared_utils = SharedUtils(
            xml_element_key="ENTRY",
            xml_subj_key="ENTSBJ",
            xml_agg_key="ENTAGG",
            xml_unavail_reason_key="ENTUNAVAILREASON",
            subject_codes=subject_codes
        )

    @staticmethod
    def get_key(xml_key: str) -> str:
        """
        Takes an XML key and returns the corresponding value.

        :param xml_key: Key of value to return
        :type xml_key: str
        :return: Value corresponding to the passed key
        :rtype: str
        """
        return {
            "ENTUNAVAILREASON": "unavailable",
            "ENTPOP": "number_of_students",
            "ENTRESPONSE": "response_not_used",
            "ENTSAMPLE": "sample_not_used",
            "ENTAGG": "aggregation_level",
            "ENTAGGYEAR": "aggregation_year",
            "ENTYEAR1": "aggregation_year_1",
            "ENTSBJ": "subject",
            "ACCESS": "access",
            "ALEVEL": "a-level",
            "BACC": "baccalaureate",
            "DEGREE": "degree",
            "FOUNDTN": "foundation",
            "NOQUALS": "none",
            "OTHER": "other_qualifications",
            "OTHERHE": "another_higher_education_qualifications",
        }.get(xml_key)

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Takes a dictionary of course data and returns a list of JSON dictionaries for the entry element.

        :param raw_course_data: Dictionary of course data
        :type raw_course_data: Dict[str, Any]
        :return: List of JSON dictionaries for the entry element
        :rtype: List[Dict[str, Any]]
        """
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)
