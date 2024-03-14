from typing import Any
from typing import Dict
from typing import List
from typing import Union

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class NhsNss:
    """Extracts and transforms the NHS NSS course element"""

    NUM_QUESTIONS = 6

    def __init__(self) -> None:
        self.xml_element_key = "NHSNSS"

        self.shared_utils = SharedUtils(
            self.xml_element_key, "NHSSBJ", "NHSAGG", "NHSUNAVAILREASON"
        )
        self.question_description_lookup = self.shared_utils.get_lookup(
            "nhs_question_description"
        )
        self.data_fields_lookup = self.shared_utils.get_lookup(
            "nhs_data_fields"
        )
        self.is_question_lookup = [
            f"NHSQ{i}" for i in range(1, NhsNss.NUM_QUESTIONS + 1)
        ]

    def is_question(self, xml_key: str) -> bool:
        return xml_key in self.is_question_lookup

    def get_question(self, xml_elem: Dict[str, Any], xml_key: str) -> Dict[str, Any]:
        question = {
            "description": self.question_description_lookup.get(xml_key)
        }
        agree_or_strongly_agree = xml_elem.get(xml_key)
        if agree_or_strongly_agree and agree_or_strongly_agree.isdigit():
            question["agree_or_strongly_agree"] = int(xml_elem.get(xml_key))
        return question

    def get_mandatory_field(self, xml_elem: Dict[str, Any], xml_key: str) -> Union[Dict[str, Any], int]:
        if self.is_question(xml_key):
            return self.get_question(xml_elem, xml_key)
        return self.shared_utils.get_json_value(xml_elem.get(xml_key))

    def get_json_data(self, xml_elem: Dict[str, Any]) -> Dict[str, Any]:
        lookup = self.data_fields_lookup
        json_data = dict()
        for xml_key in lookup:
            if lookup.get(xml_key)[1] == "M":
                json_data[lookup[xml_key][0]] = self.get_mandatory_field(
                    xml_elem, xml_key
                )
            else:
                if xml_key in xml_elem:
                    json_key = lookup.get(xml_key)[0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(
                            xml_elem
                        )
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem.get(xml_key)
                        )
        return json_data

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        for xml_elem in raw_xml_list:
            json_elem = dict()
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(
                    xml_elem
                )
            json_elem_list.append(json_elem)
        return json_elem_list
