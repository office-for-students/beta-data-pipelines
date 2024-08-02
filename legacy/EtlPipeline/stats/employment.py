from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Employment:
    """Extracts and transforms the Employment course element"""

    def __init__(self, subject_codes) -> None:
        self.shared_utils = SharedUtils(
            xml_element_key="EMPLOYMENT",
            xml_subj_key="EMPSBJ",
            xml_agg_key="EMPAGG",
            xml_unavail_reason_key="EMPUNAVAILREASON",
            subject_codes=subject_codes
        )

    @staticmethod
    def get_key(xml_key) -> str:
        """
        Takes an XML key and returns the corresponding value.

        :param xml_key: Key of value to return
        :type xml_key: str
        :return: Value corresponding to the passed key
        :rtype: str
        """
        return {
            "EMPUNAVAILREASON": "unavailable",
            "EMPPOP": "number_of_students",
            "EMPRESPONSE": "response_not_used",
            "EMPSAMPLE": "sample_not_used",
            "EMPRESP_RATE": "response_rate",
            "EMPAGG": "aggregation_level",
            "EMPAGGYEAR": "aggregation_year",
            "EMPYEAR1": "aggregation_year_1",
            "EMPSBJ": "subject",
            "WORKSTUDY": "in_work_or_study",
            "PREVWORKSTUD": "unemp_prev_emp_since_grad",
            "STUDY": "doing_further_study",
            "UNEMP": "unemp_not_work_since_grad",
            "BOTH": "in_work_and_study",
            "NOAVAIL": "other",
            "WORK": "in_work",
        }.get(xml_key)

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Takes a dictionary of course data and returns a list of JSON dictionaries for the employment element.

        :param raw_course_data: Dictionary of course data
        :type raw_course_data: Dict[str, Any]
        :return: List of JSON dictionaries for the employment element
        :rtype: List[Dict[str, Any]]
        """
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)
