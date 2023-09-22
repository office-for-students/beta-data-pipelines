from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from EtlPipeline.course_stats import get_earnings_unavail_text
from EtlPipeline.mappings.base import BaseMappings
from EtlPipeline.utils import get_earnings_agg_unavail_messages


class GoInstitutionMappings(BaseMappings):
    OPTIONS = ["GO"]
    unavailable_keys = ["unavail_reason"]

    def get_mappings(self) -> List[Tuple[str, str]]:
        return [
            (f'{self.mapping_id}SALUNAVAILREASON', "unavail_reason"),
            (f'{self.mapping_id}SALPOP', "pop"),
            (f'{self.mapping_id}SALAGGYEAR', "aggregation_year"),
            (f'{self.mapping_id}SALRESP_RATE', "resp_rate"),
            (f'{self.mapping_id}SALAGG', "agg"),
            (f'{self.mapping_id}SALSBJ', "subject"),
            (f'{self.mapping_id}INSTLQ', "lq"),
            (f'{self.mapping_id}INSTMED', "med"),
            (f'{self.mapping_id}INSTUQ', "uq"),
            (f'{self.mapping_id}PROV_PC_UK', "inst_prov_pc_uk"),
            (f'{self.mapping_id}PROV_PC_E', "inst_prov_pc_e"),
            (f'{self.mapping_id}PROV_PC_NI', "inst_prov_pc_ni"),
            (f'{self.mapping_id}PROV_PC_S', "inst_prov_pc_s"),
            (f'{self.mapping_id}PROV_PC_W', "inst_prov_pc_w")
        ]

    def custom_unavailable(self, json_data: Dict[str, Any], elem: Dict[str, Any], key: str) -> None:
        json_data["unavail_reason"] = elem["GOSALUNAVAILREASON"]
        json_data["unavail_text_english"], json_data["unavail_text_welsh"] = get_earnings_unavail_text(
            "institution",
            "go",
            json_data["unavail_reason"]
        )

    def final_unavailable(self, json_data):
        if 'agg' in json_data and 'subject' in json_data:
            json_data["earnings_agg_unavail_message"] = get_earnings_agg_unavail_messages(
                json_data["agg"],
                json_data["subject"]
            )
