from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from legacy.EtlPipeline.course_stats import get_earnings_unavail_text
from legacy.EtlPipeline.mappings.base import BaseMappings
from legacy.EtlPipeline.subject_enricher import SubjectCourseEnricher


class GoSalaryMappings(BaseMappings):
    """GO Mappings for salaries"""
    OPTIONS = ["GO"]
    unavailable_keys = []

    def __init__(self, mapping_id: str, subject_enricher: SubjectCourseEnricher):
        super().__init__(mapping_id=mapping_id, subject_enricher=subject_enricher)

    def get_mappings(self) -> List[Tuple[str, str]]:
        """
        Returns the list of mappings as a list of tuples with the class' mapping ID.

        :return: List of mappings
        :rtype: List[Tuple[str, str]]
        """
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

    def per_course_unavailable(self, json_data: Dict[str, Any], subject_codes: dict[str, dict[str, str]]) -> None:
        """
        Takes a JSON as a dictionary and sets the unavailable texts for both languages.

        :param json_data: JSON data as a dictionary
        :type json_data: Dict[str, Any]
        :param subject_codes: Subject codes for shared utils object
        :type subject_codes: dict[str, dict[str, str]]
        :return: None
        """
        json_data["unavail_text_region_not_exists_english"], json_data["unavail_text_region_not_exists_welsh"] = \
            get_earnings_unavail_text(
                subject_codes=subject_codes,
                inst_or_sect="sector",
                data_source="go",
                key_level_3="region_not_exists"
            )
        json_data["unavail_text_region_not_nation_english"], json_data["unavail_text_region_not_nation_welsh"] = \
            get_earnings_unavail_text(
                subject_codes=subject_codes,
                inst_or_sect="sector",
                data_source="go",
                key_level_3="region_not_nation"
            )
