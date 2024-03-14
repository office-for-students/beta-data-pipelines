from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Employment:
    """Extracts and transforms the Employment course element"""

    def __init__(self) -> None:
        self.xml_element_key = "EMPLOYMENT"
        self.xml_subj_key = "EMPSBJ"
        self.xml_agg_key = "EMPAGG"
        self.xml_unavail_reason_key = "EMPUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key) -> str:
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
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)