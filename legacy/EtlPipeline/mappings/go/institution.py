from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

from legacy.EtlPipeline.course_stats import get_earnings_unavail_text
from legacy.EtlPipeline.mappings.base import BaseMappings
from legacy.EtlPipeline.utils import get_earnings_agg_unavail_messages


class GoInstitutionMappings(BaseMappings):
    """GO Mappings for institutions"""
    OPTIONS = ["GO"]
    unavailable_keys = ["unavail_reason"]

    def get_mappings(self) -> List[Tuple[str, str]]:
        """
        Returns the list of mappings as a list of tuples with the class' mapping ID.

        :return: List of mappings
        :rtype: List[Tuple[str, str]]
        """
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

    def custom_unavailable(self, subject_codes: dict[str, dict[str, str]], json_data: Dict[str, Any], elem: Dict[str, Any], key: str = None) -> None:
        """
        Takes a JSON as a dictionary and an element containing unavailable reasons, and sets the unavailable
        reason in the JSON

        :param subject_codes: Subject codes for shared utils object
        :type subject_codes: dict[str, dict[str, str]]
        :param json_data: JSON data to set unavailable reason of
        :type json_data: Dict[str, Any]
        :param elem: Element containing custom unavailable reason
        :type elem: Dict[str, Any]
        :param key: Not required
        :return: None
        """
        json_data["unavail_reason"] = elem["GOSALUNAVAILREASON"]
        json_data["unavail_text_english"], json_data["unavail_text_welsh"] = get_earnings_unavail_text(
            subject_codes=subject_codes,
            inst_or_sect="institution",
            data_source="go",
            key_level_3=json_data["unavail_reason"]
        )

    def final_unavailable(self, json_data: Dict[str, Any]) -> None:
        """
        Takes a JSON as a dictionary and sets the unavailable reason according to the aggregation and subject levels

        :param json_data: JSON to apply unavailable reasons to
        :type json_data: Dict[str, Any]
        :return: None
        """
        if 'agg' in json_data and 'subject' in json_data:
            json_data["earnings_agg_unavail_message"] = get_earnings_agg_unavail_messages(
                json_data["agg"],
                json_data["subject"]
            )
