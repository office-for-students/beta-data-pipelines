from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class JobType:
    """Extracts and transforms the JobType course element"""

    def __init__(self) -> None:
        self.xml_element_key = "JOBTYPE"
        self.xml_subj_key = "JOBSBJ"
        self.xml_agg_key = "JOBAGG"
        self.xml_unavail_reason_key = "JOBUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key: str) -> str:
        return {
            "JOBUNAVAILREASON": "unavailable",
            "JOBPOP": "number_of_students",
            "JOBRESPONSE": "response_not_used",
            "JOBSAMPLE": "sample_not_used",
            "JOBAGG": "aggregation_level",
            "JOBAGGYEAR": "aggregation_year",
            "JOBYEAR1": "aggregation_year_1",
            "JOBSBJ": "subject",
            "JOBRESP_RATE": "response_rate",
            "PROFMAN": "professional_or_managerial_jobs",
            "OTHERJOB": "non_professional_or_managerial_jobs",
            "UNKWN": "unknown_professions",
        }.get(xml_key)

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)
