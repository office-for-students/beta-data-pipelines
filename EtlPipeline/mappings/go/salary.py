from typing import List
from typing import Tuple

from EtlPipeline.course_stats import get_earnings_unavail_text
from EtlPipeline.mappings.base import BaseMappings


class GoSalaryMappings(BaseMappings):
    OPTIONS = ["GO"]
    unavailable_keys = []
    unavailable_method = get_earnings_unavail_text

    def __init__(self, mapping_id, subject_enricher):
        super().__init__(mapping_id=mapping_id, subject_enricher=subject_enricher)

    def get_mappings(self) -> List[Tuple[str, str]]:
        return [
            (f'{self.mapping_id}SECSBJ', "subject"),
            ('KISMODE', "mode"),
            ('KISLEVEL', "level"),
            (f'{self.mapping_id}SECLQ_UK', "lq_uk"),
            (f'{self.mapping_id}SECMED_UK', "med_uk"),
            (f'{self.mapping_id}SECUQ_UK', "uq_uk"),
            (f'{self.mapping_id}SECPOP_UK', "pop_uk"),
            (f'{self.mapping_id}SECRESP_UK', "resp_uk"),
            (f'{self.mapping_id}SECLQ_E', "lq_e"),
            (f'{self.mapping_id}SECMED_E', "med_e"),
            (f'{self.mapping_id}SECUQ_E', "uq_e"),
            (f'{self.mapping_id}SECPOP_E', "pop_e"),
            (f'{self.mapping_id}SECRESP_E', "resp_e"),
            (f'{self.mapping_id}SECLQ_S', "lq_s"),
            (f'{self.mapping_id}SECMED_S', "med_s"),
            (f'{self.mapping_id}SECUQ_S', "uq_s"),
            (f'{self.mapping_id}SECPOP_S', "pop_s"),
            (f'{self.mapping_id}SECRESP_S', "resp_s"),
            (f'{self.mapping_id}SECLQ_W', "lq_w"),
            (f'{self.mapping_id}SECMED_W', "med_w"),
            (f'{self.mapping_id}SECUQ_W', "uq_w"),
            (f'{self.mapping_id}SECPOP_W', "pop_w"),
            (f'{self.mapping_id}SECRESP_W', "resp_w"),
            (f'{self.mapping_id}SECLQ_NI', "lq_ni"),
            (f'{self.mapping_id}SECMED_NI', "med_ni"),
            (f'{self.mapping_id}SECUQ_NI', "uq_ni"),
            (f'{self.mapping_id}SECPOP_NI', "pop_ni"),
            (f'{self.mapping_id}SECRESP_NI', "resp_ni"),
        ]
