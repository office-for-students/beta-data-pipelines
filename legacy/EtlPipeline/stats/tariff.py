from typing import Any
from typing import Dict
from typing import List

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Tariff:
    """Extracts and transforms the Tariff course element"""

    def __init__(self) -> None:
        self.xml_element_key = "TARIFF"
        self.xml_subj_key = "TARSBJ"
        self.xml_agg_key = "TARAGG"
        self.xml_agg_year = "TARAGGYEAR"
        self.xml_pop_key = "TARPOP"
        self.xml_unavail_reason_key = "TARUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )
        self.tariff_description_lookup = self.shared_utils.get_lookup(
            "tariff_description"
        )

    def get_tariff_description(self, xml_key: str) -> Dict[str, Any]:
        return self.tariff_description_lookup.get(xml_key)

    def get_tariffs_list(self, xml_elem: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {
                "code": xml_key,
                "description": self.get_tariff_description(xml_key),
                "entrants": int(xml_elem.get(xml_key, 0)),
            }
            for xml_key in self.tariff_description_lookup.keys()
        ]

    def get_json_data(self, xml_elem: Dict[str, Any]) -> Dict[str, Any]:
        json_data = {}
        agg_level = xml_elem.get(self.xml_agg_key)
        population = xml_elem.get(self.xml_pop_key)
        if agg_level:
            json_data["aggregation_level"] = int(agg_level)
        if population:
            json_data["number_of_students"] = int(population)
        json_data["aggregation_year"] = xml_elem.get(self.xml_agg_year)
        if self.xml_subj_key in xml_elem:
            json_data["subject"] = self.shared_utils.get_subject(xml_elem)
        json_data["tariffs"] = self.get_tariffs_list(xml_elem)
        return json_data

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(
                    xml_elem
                )
            # sorted_json_elem = dict(json_elem.items())
            json_elem_list.append(json_elem)
        return json_elem_list
