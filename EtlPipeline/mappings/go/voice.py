from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from EtlPipeline.mappings.base import BaseMappings
from EtlPipeline.utils import get_go_work_unavail_messages


class GoVoiceMappings(BaseMappings):
    OPTIONS = ["GO"]
    unavailable_keys = ["unavailable"]

    def __init__(self, mapping_id, subject_enricher):
        super().__init__(mapping_id=mapping_id, subject_enricher=subject_enricher)

    def get_mappings(self) -> List[Tuple[str, str]]:
        return [
            (f'{self.mapping_id}WORKSBJ', "subject"),
            (f'{self.mapping_id}WORKAGG', "go_work_agg"),
            (f'{self.mapping_id}WORKAGGYEAR', "aggregation_year"),
            (f'{self.mapping_id}WORKYEAR1', "aggregation_year_1"),
            (f'{self.mapping_id}WORKYEAR2', "aggregation_year_2"),
            (f'{self.mapping_id}WORKSKILLS', "go_work_skills"),
            (f'{self.mapping_id}WORKMEAN', "go_work_mean"),
            (f'{self.mapping_id}WORKONTRACK', "go_work_on_track"),
            (f'{self.mapping_id}WORKPOP', "go_work_pop"),
            (f'{self.mapping_id}WORKRESP_RATE', "go_work_resp_rate"),
            (f'{self.mapping_id}WORKUNAVAILREASON', "unavailable")
        ]

    #
    def custom_unavailable(self, json_data: Dict[str, Any], elem, key: str) -> None:
        json_data["unavailable"] = get_go_work_unavail_messages(
            xml_element_key="GO",
            xml_agg_key='GOWORKAGG',
            xml_unavail_reason_key="GOWORKUNAVAILREASON",
            raw_data_element=elem
        )
