from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Continuation:
    """Extracts and transforms the Continuation course element"""

    def __init__(self) -> None:
        self.xml_element_key = "CONTINUATION"
        self.xml_subj_key = "CONTSBJ"
        self.xml_agg_key = "CONTAGG"
        self.xml_unavail_reason_key = "CONTUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key) -> str:
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
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)