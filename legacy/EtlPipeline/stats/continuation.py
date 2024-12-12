from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Continuation:
    """Extracts and transforms the Continuation course element"""

    def __init__(self, subject_codes) -> None:
        self.shared_utils = SharedUtils(
            xml_element_key="CONTINUATION",
            xml_subj_key="CONTSBJ",
            xml_agg_key="CONTAGG",
            xml_unavail_reason_key="CONTUNAVAILREASON",
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
            "CONTUNAVAILREASON": "unavailable",
            "CONTPOP": "number_of_students",
            "CONTAGG": "aggregation_level",
            "CONTAGGYEAR": "aggregation_year",
            "CONTYEAR1": "aggregation_year_1",
            "CONTSBJ": "subject",
            "UCONT": "continuing_with_provider",
            "UDORMANT": "dormant",
            "UGAINED": "gained",
            "ULEFT": "left",
            "ULOWER": "lower",
        }.get(xml_key)

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Takes a dictionary of course data and returns a list of JSON dictionaries for the continuation element.

        :param raw_course_data: Dictionary of course data
        :type raw_course_data: Dict[str, Any]
        :return: List of JSON dictionaries for the continuation element
        :rtype: List[Dict[str, Any]]
        """
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)
