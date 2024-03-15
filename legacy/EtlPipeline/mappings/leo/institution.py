from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from legacy.EtlPipeline.course_stats import get_earnings_unavail_text
from legacy.EtlPipeline.mappings.base import BaseMappings
from legacy.EtlPipeline.utils import get_earnings_agg_unavail_messages


class LeoInstitutionMappings(BaseMappings):
    OPTIONS = ["LEO3", "LEO5"]
    unavailable_keys = ["unavail_reason"]

    def __init__(self, mapping_id, subject_enricher):
        super().__init__(mapping_id=mapping_id, subject_enricher=subject_enricher)

    def get_mappings(self) -> List[Tuple[str, str]]:
        return [
            (f"{self.mapping_id}UNAVAILREASON", "unavail_reason"),
            (f'{self.mapping_id}POP', "pop"),
            (f'{self.mapping_id}AGG', "agg"),
            (f'{self.mapping_id}AGGYEAR', "aggregation_year"),
            (f'{self.mapping_id}SBJ', "subject"),
            (f'{self.mapping_id}INSTLQ', "lq"),
            (f'{self.mapping_id}INSTMED', "med"),
            (f'{self.mapping_id}INSTUQ', "uq"),
            (f'{self.mapping_id}PROV_PC_UK', "inst_prov_pc_uk"),
            (f'{self.mapping_id}PROV_PC_E', "inst_prov_pc_e"),
            (f'{self.mapping_id}PROV_PC_NI', "inst_prov_pc_ni"),
            (f'{self.mapping_id}PROV_PC_S', "inst_prov_pc_s"),
            (f'{self.mapping_id}PROV_PC_W', "inst_prov_pc_w"),
            (f'{self.mapping_id}PROV_PC_NW', "inst_prov_pc_nw"),
            (f'{self.mapping_id}PROV_PC_NE', "inst_prov_pc_ne"),
            (f'{self.mapping_id}PROV_PC_EM', "inst_prov_pc_em"),
            (f'{self.mapping_id}PROV_PC_WM', "inst_prov_pc_wm"),
            (f'{self.mapping_id}PROV_PC_EE', "inst_prov_pc_ee"),
            (f'{self.mapping_id}PROV_PC_SE', "inst_prov_pc_se"),
            (f'{self.mapping_id}PROV_PC_SW', "inst_prov_pc_sw"),
            (f'{self.mapping_id}PROV_PC_YH', "inst_prov_pc_yh"),
            (f'{self.mapping_id}PROV_PC_LN', "inst_prov_pc_lo"),
            (f'{self.mapping_id}PROV_PC_ED', "inst_prov_pc_ed"),
            (f'{self.mapping_id}PROV_PC_GL', "inst_prov_pc_gl"),
            (f'{self.mapping_id}PROV_PC_CF', "inst_prov_pc_cf")
        ]

    def final_unavailable(self, json_data):
        if self.mapping_id == "LEO3":
            if 'agg' in json_data and 'subject' in json_data:
                json_data["earnings_agg_unavail_message"] = get_earnings_agg_unavail_messages(
                    json_data["agg"],
                    json_data["subject"]
                )

    def custom_unavailable(self, json_data: Dict[str, Any], key: str, elem: Optional[List] = None) -> None:
        json_data["unavail_text_english"], json_data["unavail_text_welsh"] = get_earnings_unavail_text(
            inst_or_sect="institution",
            data_source="leo",
            key_level_3=json_data[key]
        )
