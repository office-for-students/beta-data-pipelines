import json
import logging
import os
import unicodedata
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Union

from legacy.EtlPipeline.validators import validate_agg
from legacy.EtlPipeline.validators import validate_unavailable_reason_code
from legacy.services.dataset_service import DataSetService
from legacy.services.utils import get_subject_lookups


class SharedUtils:
    """Functionality required by several stats related classes"""

    try:
        dsh = DataSetService()  # TODO: warning presented as dsh wasn't assigned, but the catch all
        # Exception below would have masked this - leading to no subject codes.
        version = dsh.get_latest_version_number()
        subj_codes = get_subject_lookups(version)
        logging.info("Using database subject codes.")

    except Exception:
        logging.info("Using local subject codes.")
        subj_codes = {}

    def __init__(
            self,
            xml_element_key,
            xml_subj_key,
            xml_agg_key,
            xml_unavail_reason_key,
    ) -> None:

        self.xml_element_key = xml_element_key
        self.xml_subj_key = xml_subj_key
        self.xml_agg_key = xml_agg_key
        self.xml_unavail_reason_key = xml_unavail_reason_key
        self.subj_code_english = self.get_lookup("subj_code_english")
        self.subj_code_welsh = self.get_lookup("subj_code_welsh")
        self.unavail_reason_english = self.get_lookup("unavail_reason_english")
        self.unavail_reason_welsh = self.get_lookup("unavail_reason_welsh")

    @staticmethod
    def get_lookup(lookup_name: str) -> Dict[str, Any]:
        cwd = os.path.dirname(os.path.abspath(__file__))
        filename = {
            "subj_code_english": "subj_code_english.json",
            "subj_code_welsh": "subj_code_welsh.json",
            "unavail_reason_english": "unavailreason_english.json",
            "unavail_reason_welsh": "unavailreason_welsh.json",
            "tariff_description": "tariff_description.json",
            # "salary_data_fields": "salary_data_fields.json",
            "nss_question_description": "nss_question_description.json",
            "nss_data_fields": "nss_data_fields.json",
            "nhs_question_description": "nhs_question_description.json",
            "nhs_data_fields": "nhs_data_fields.json",
            # "leo_unavail_reason_english": "leo_unavailreason_english.json",
            # "leo_unavail_reason_welsh": "leo_unavailreason_welsh.json",
            # "leo_data_fields": "leo_data_fields.json",
            "common_data_fields": "common_data_fields.json",
            "earnings_unavail_reason_english": "earnings_unavailreason_english.json",
            "earnings_unavail_reason_welsh": "earnings_unavailreason_welsh.json",
        }.get(lookup_name)
        with open(os.path.join(cwd, f"lookup_files/{filename}")) as infile:
            return json.load(infile)

    def get_subject(self, xml_elem: Dict[str, Any]) -> Dict[str, Any]:
        subj_key = xml_elem[self.xml_subj_key]
        subject = {
            "code": subj_key,
            "english_label": self.get_english_sbj_label(subj_key),
            "welsh_label": self.get_welsh_sbj_label(subj_key)
        }
        return subject

    def get_english_sbj_label(self, code: str) -> Dict[str, Any]:
        if SharedUtils.subj_codes != {}:
            return SharedUtils.subj_codes[code].get("english_name")
        return self.subj_code_english.get(code)

    def get_welsh_sbj_label(self, code: str) -> Dict[str, Any]:
        if SharedUtils.subj_codes != {}:
            return SharedUtils.subj_codes[code].get("welsh_name")
        return self.subj_code_welsh.get(code)

    def get_json_list(self, raw_course_data: Dict[str, Any], get_key: Callable) -> List[Dict[str, Any]]:
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )

        for xml_elem in raw_xml_list:
            json_elem = {}
            for xml_key in xml_elem:
                json_key = get_key(xml_key)
                if json_key == "subject":
                    json_elem[json_key] = self.get_subject(xml_elem)
                elif json_key == "unavailable":
                    if self.need_unavailable(xml_elem):
                        json_elem[json_key] = self.get_unavailable(xml_elem)
                else:
                    json_elem[json_key] = self.get_json_value(
                        xml_elem.get(xml_key)
                    )
            try:
                ordered_json_elem = dict(sorted(json_elem.items()))
                json_elem_list.append(ordered_json_elem)
            except:
                json_elem_list.append(json_elem)
        return json_elem_list

    def need_unavailable(self, xml_elem: Dict[str, Any]) -> bool:
        if not self.has_data(xml_elem):
            return True

        unavail_reason_code = xml_elem.get(self.xml_unavail_reason_key)
        agg = xml_elem.get(self.xml_agg_key)
        agg_codes = self.get_aggs_for_code(unavail_reason_code)
        if agg in agg_codes:
            return True
        return False

    def get_aggs_for_code(self, unavail_reason_code: str) -> List[str]:
        return self.unavail_reason_english.get("data").get(unavail_reason_code).keys()

    def get_unavailable(self, elem: Dict[str, Any]) -> Union[Dict[str, Any], str]:
        unavailable = {}
        subj_key = elem.get(self.xml_subj_key)
        agg = elem.get(self.xml_agg_key) if self.has_data(elem) else None
        if agg == "14":
            return ""

        unavail_reason_code = elem.get(self.xml_unavail_reason_key)
        validate_unavailable_reason_code(unavail_reason_code)
        unavailable["code"] = int(unavail_reason_code)

        # Determine if elem (an ordered dictionary) contains a key that ends in 'RESP_RATE', e.g. 'EMPRESP_RATE'.
        resp_rate_state = "any"
        if unavailable["code"] == 0:
            if any([x for x in elem if 'RESP_RATE' in x]):
                resp_rate_state = "yes_resp_rate"
            else:
                resp_rate_state = "no_resp_rate"

        unavailable["reason_english"] = self.get_unavailable_reason_str(
            unavail_reason_code, subj_key, agg, elem, resp_rate_state
        )
        unavailable["reason_welsh"] = self.get_unavailable_reason_str(
            unavail_reason_code, subj_key, agg, elem, resp_rate_state, welsh=True
        )
        return unavailable

    def get_unavailable_reason_str(
            self,
            unavail_reason_code: str,
            subj_key: str,
            agg: str,
            xml_elem: Dict[str, Any],
            resp_rate_state: str,
            welsh: bool = False
    ):
        validate_unavailable_reason_code(unavail_reason_code)
        if welsh:
            unavail_reason_lookup = self.unavail_reason_welsh
        else:
            unavail_reason_lookup = self.unavail_reason_english

        if not self.has_data(xml_elem):
            reason_str = unavail_reason_lookup["no-data"].get(unavail_reason_code)
            return unicodedata.normalize("NFKD", reason_str)

        validate_agg(unavail_reason_code, agg, unavail_reason_lookup)

        # if unavail_reason_code == 0 and agg and agg != "14" and agg != "" and agg is not None:
        #     partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code][agg][resp_rate_state]
        # elif unavail_reason_code != 0:
        #     partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code][agg]
        # The lookup tables do not contain entries for code=0, agg=14 or code=0, agg=None.
        #    No unavail message needs to be displayed in either of these scenarios.
        if unavail_reason_code == '0':
            partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code].get(agg).get(resp_rate_state)
        else:
            partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code].get(agg)

        partial_reason_str = unicodedata.normalize("NFKD", partial_reason_str)
        if welsh:
            subj = self.get_unavailable_reason_subj_welsh(subj_key)
        else:
            subj = self.get_unavailable_reason_subj_english(subj_key)
        return partial_reason_str.replace("[Subject]", subj)

    def get_unavailable_reason_subj_english(self, sbj_key: str) -> Dict[str, Any]:
        if sbj_key:
            return self.get_english_sbj_label(sbj_key)
        return self.unavail_reason_english["no-subject"]

    def get_unavailable_reason_subj_welsh(self, sbj_key: str) -> Dict[str, Any]:
        if sbj_key:
            return self.get_welsh_sbj_label(sbj_key)
        return self.unavail_reason_welsh["no-subject"]

    @staticmethod
    def has_data(xml_elem: Dict[str, Any]) -> bool:
        """Returns True if the stats XML element has data otherwise False"""
        return xml_elem is not None and len(xml_elem) > 1

    @staticmethod
    def get_json_value(xml_value: str) -> int:
        if xml_value and xml_value.isdigit():
            xml_value = int(xml_value)
        return xml_value

    @staticmethod
    def get_raw_list(data: Dict[str, Any], element_key: str) -> Union[List[Dict[str, Any]], str]:
        """If value is a dict, return in list, otherwise return value"""
        if isinstance(data.get(element_key), dict):
            return [data.get(element_key)]
        return data.get(element_key)
