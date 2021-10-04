from typing import List
from typing import Tuple

from EtlPipeline.mappings.base import BaseMappings


class LeoSalaryMappings(BaseMappings):
    OPTIONS = ["LEO3", "LEO5"]

    def __init__(self, mapping_id):
        super().__init__(mapping_id=mapping_id)

    def get_mappings(self) -> List[Tuple[str, str]]:
        return [

            ('KISMODE', "mode"),
            ('KISLEVEL', "level"),
            (f'{self.mapping_id}LQ_UK', "lq_uk"),
            (f'{self.mapping_id}MED_UK', "med_uk"),
            (f'{self.mapping_id}UQ_UK', "uq_uk"),
            (f'{self.mapping_id}SECPOP_UK', "pop_uk"),
            (f'{self.mapping_id}LQ_E', "lq_e"),
            (f'{self.mapping_id}MED_E', "med_e"),
            (f'{self.mapping_id}UQ_E', "uq_e"),
            (f'{self.mapping_id}SECPOP_E', "pop_e"),
            (f'{self.mapping_id}LQ_S', "lq_s"),
            (f'{self.mapping_id}MED_S', "med_s"),
            (f'{self.mapping_id}UQ_S', "uq_s"),
            (f'{self.mapping_id}SECPOP_S', "pop_s"),
            (f'{self.mapping_id}LQ_W', "lq_w"),
            (f'{self.mapping_id}MED_W', "med_w"),
            (f'{self.mapping_id}UQ_W', "uq_w"),
            (f'{self.mapping_id}SECPOP_W', "pop_w"),
            (f'{self.mapping_id}LQ_NW', "lq_nw"),
            (f'{self.mapping_id}MED_NW', "med_nw"),
            (f'{self.mapping_id}UQ_NW', "uq_nw"),
            (f'{self.mapping_id}SECPOP_NW', "pop_nw"),
            (f'{self.mapping_id}LQ_NE', "lq_ne"),
            (f'{self.mapping_id}MED_NE', "med_ne"),
            (f'{self.mapping_id}UQ_NE', "uq_ne"),
            (f'{self.mapping_id}SECPOP_NE', "pop_ne"),
            (f'{self.mapping_id}LQ_EM', "lq_em"),
            (f'{self.mapping_id}MED_EM', "med_em"),
            (f'{self.mapping_id}UQ_EM', "uq_em"),
            (f'{self.mapping_id}SECPOP_EM', "pop_em"),
            (f'{self.mapping_id}LQ_WM', "lq_wm"),
            (f'{self.mapping_id}MED_WM', "med_wm"),
            (f'{self.mapping_id}UQ_WM', "uq_wm"),
            (f'{self.mapping_id}SECPOP_WM', "pop_wm"),
            (f'{self.mapping_id}LQ_EE', "lq_ee"),
            (f'{self.mapping_id}MED_EE', "med_ee"),
            (f'{self.mapping_id}UQ_EE', "uq_ee"),
            (f'{self.mapping_id}SECPOP_EE', "pop_ee"),
            (f'{self.mapping_id}LQ_SE', "lq_se"),
            (f'{self.mapping_id}MED_SE', "med_se"),
            (f'{self.mapping_id}UQ_SE', "uq_se"),
            (f'{self.mapping_id}SECPOP_SE', "pop_se"),
            (f'{self.mapping_id}LQ_SW', "lq_sw"),
            (f'{self.mapping_id}MED_SW', "med_sw"),
            (f'{self.mapping_id}UQ_SW', "uq_sw"),
            (f'{self.mapping_id}SECPOP_SW', "pop_sw"),
            (f'{self.mapping_id}LQ_YH', "lq_yh"),
            (f'{self.mapping_id}MED_YH', "med_yh"),
            (f'{self.mapping_id}UQ_YH', "uq_yh"),
            (f'{self.mapping_id}SECPOP_YH', "pop_yh"),
            (f'{self.mapping_id}LQ_LN', "lq_lo"),
            (f'{self.mapping_id}MED_LN', "med_lo"),
            (f'{self.mapping_id}UQ_LN', "uq_lo"),
            (f'{self.mapping_id}SECPOP_LN', "pop_lo"),
            (f'{self.mapping_id}LQ_ED', "lq_ed"),
            (f'{self.mapping_id}MED_ED', "med_ed"),
            (f'{self.mapping_id}UQ_ED', "uq_ed"),
            (f'{self.mapping_id}SECPOP_ED', "pop_ed"),
            (f'{self.mapping_id}LQ_GL', "lq_gl"),
            (f'{self.mapping_id}MED_GL', "med_gl"),
            (f'{self.mapping_id}UQ_GL', "uq_gl"),
            (f'{self.mapping_id}SECPOP_GL', "pop_gl"),
            (f'{self.mapping_id}LQ_CF', "lq_cf"),
            (f'{self.mapping_id}MED_CF', "med_cf"),
            (f'{self.mapping_id}UQ_CF', "uq_cf"),
            (f'{self.mapping_id}SECPOP_CF', "pop_cf")
        ]
