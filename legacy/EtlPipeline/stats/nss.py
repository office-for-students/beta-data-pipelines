import logging
from collections import OrderedDict
from typing import Any
from typing import Dict
from typing import List
from typing import Union

from legacy.EtlPipeline.stats.shared_utils import SharedUtils


class Nss:
    """Extracts and transforms the NSS course element"""

    NUM_QUESTIONS = 28

    def __init__(self) -> None:
        self.xml_element_key = "NSS"

        self.shared_utils = SharedUtils(
            self.xml_element_key, "NSSSBJ", "NSSAGG", "NSSUNAVAILREASON"
        )
        self.question_lookup = self.shared_utils.get_lookup(
            "nss_question_description"
        )
        self.nss_data_fields_lookup = self.shared_utils.get_lookup(
            "nss_data_fields"
        )
        self.is_question_lookup = [
            f"Q{i}" for i in range(1, Nss.NUM_QUESTIONS + 1)
        ]

    def is_question(self, xml_key: str) -> bool:
        return xml_key in self.is_question_lookup

    def get_question(self, xml_elem: Dict[str, Any], xml_key: str) -> Dict[str, Any]:
        question = {
            "description": self.question_lookup.get(xml_key)
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
        lookup = self.nss_data_fields_lookup
        json_data = dict()
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                try:
                    json_data[lookup[xml_key][0]] = self.get_mandatory_field(
                        xml_elem, xml_key
                    )
                except KeyError as e:
                    logging.info("this institution has no nss data except unavailable reason")
            else:
                if xml_key in xml_elem:
                    json_key = lookup.get(xml_key)[0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(
                            xml_elem
                        )
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_stats(self, raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        nss_country_data = raw_course_data.get("NSSCOUNTRY")

        if isinstance(nss_country_data, OrderedDict):
            for key, value in nss_country_data.items():
                if isinstance(raw_xml_list[0], OrderedDict):
                    raw_xml_list[0][key] = value
        elif isinstance(nss_country_data, list):
            for index, data_dict in enumerate(nss_country_data):
                updated_data_dict = OrderedDict(data_dict)
                for key, value in data_dict.items():
                    if index == len(raw_xml_list):
                        updated_data_dict["NSSUNAVAILREASON"] = data_dict.get("NSSCOUNTRYUNAVAILREASON")
                        updated_data_dict["NSSAGG"] = data_dict.get("NSSCOUNTRYAGG")
                        updated_data_dict["NSSSBJ"] = data_dict.get("NSSCOUNTRYSBJ")
                        raw_xml_list.append(updated_data_dict)
                    elif isinstance(raw_xml_list[index], OrderedDict):
                        raw_xml_list[index][key] = value

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
