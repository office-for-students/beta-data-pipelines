from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from legacy.EtlPipeline.mappings.base import BaseMappings
from legacy.EtlPipeline.subject_enricher import SubjectCourseEnricher
from legacy.EtlPipeline.utils import get_go_work_unavail_messages


class GoVoiceMappings(BaseMappings):
    """GO Mappings for voices"""
    OPTIONS = ["GO"]
    unavailable_keys = ["unavailable"]

    def __init__(self, mapping_id: str, subject_enricher: SubjectCourseEnricher):
        super().__init__(mapping_id=mapping_id, subject_enricher=subject_enricher)

    def get_mappings(self) -> List[Tuple[str, str]]:
        """
        Returns the list of mappings as a list of tuples with the class' mapping ID.

        :return: List of mappings
        :rtype: List[Tuple[str, str]]
        """
        return [
            (f'{self.mapping_id}WORKSBJ', "subject"),
            (f'{self.mapping_id}WORKAGG', "go_work_agg"),
            (f'{self.mapping_id}WORKAGGYEAR', "aggregation_year"),
            (f'{self.mapping_id}WORKSKILLS', "go_work_skills"),
            (f'{self.mapping_id}WORKMEAN', "go_work_mean"),
            (f'{self.mapping_id}WORKONTRACK', "go_work_on_track"),
            (f'{self.mapping_id}WORKPOP', "go_work_pop"),
            (f'{self.mapping_id}WORKRESP_RATE', "go_work_resp_rate"),
            (f'{self.mapping_id}WORKUNAVAILREASON', "unavailable")
        ]

    def custom_unavailable(self, json_data: Dict[str, Any], elem: Dict[str, Any], key: str) -> None:
        """
        Takes a JSON and a raw data element as dictionaries and sets the appropriate unavailable message

        :param json_data: JSON data
        :type json_data: Dict[str, Any]
        :param elem: Raw data element used to get unavailable message
        :type elem: Dict[str, Any]
        :param key: Not required
        :return: None
        """
        json_data["unavailable"] = get_go_work_unavail_messages(
            xml_element_key="GO",
            xml_agg_key='GOWORKAGG',
            xml_unavail_reason_key="GOWORKUNAVAILREASON",
            raw_data_element=elem
        )
