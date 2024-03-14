from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Entry:
    """Extracts and transforms the Entry course element"""

    def __init__(self) -> None:
        self.xml_element_key = "ENTRY"
        self.xml_subj_key = "ENTSBJ"
        self.xml_agg_key = "ENTAGG"
        self.xml_unavail_reason_key = "ENTUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key: str) -> str:
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
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)
