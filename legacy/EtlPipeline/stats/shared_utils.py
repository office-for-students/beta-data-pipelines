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


class SharedUtils:
    """Functionality required by several stats related classes"""

    def __init__(
            self,
            subject_codes: Dict[str, Dict[str, str]],
            xml_element_key: str,
            xml_subj_key: str,
            xml_agg_key: str,
            xml_unavail_reason_key: str,
    ) -> None:
        self.xml_element_key = xml_element_key
        self.xml_subj_key = xml_subj_key
        self.xml_agg_key = xml_agg_key
        self.xml_unavail_reason_key = xml_unavail_reason_key
        self.subj_code_english = self.get_lookup("subj_code_english")
        self.subj_code_welsh = self.get_lookup("subj_code_welsh")
        self.unavail_reason_english = self.get_lookup("unavail_reason_english")
        self.unavail_reason_welsh = self.get_lookup("unavail_reason_welsh")

        self.subj_codes = subject_codes
        logging.info("Using database subject codes.")

    @staticmethod
    def get_lookup(lookup_name: str) -> Dict[str, Any]:
        """
        Takes a lookup name and returns the corresponding file loaded as a JSON dictionary

        :param lookup_name: Lookup value associated with the desired JSON file
        :type lookup_name: str
        :return: Corresponding JSON file as a dictionary
        :rtype: Dict[str, Any]
        """
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
        """
        Takes an XML element and returns the subject associated with the SharedUtils' subject key.

        :param xml_elem: XML element containing desired subject data
        :type xml_elem: Dict[str, Any]
        :return: Constructed subject dictionary
        :rtype: Dict[str, Any]
        """
        subj_key = xml_elem[self.xml_subj_key]
        subject = {
            "code": subj_key,
            "english_label": self.get_english_sbj_label(subj_key),
            "welsh_label": self.get_welsh_sbj_label(subj_key)
        }
        return subject

    def get_english_sbj_label(self, code: str) -> str:
        """
        Takes a subject code and returns the english name for the associated subject.

        :param code: Subject code to perform lookup with
        :type code: str
        :return: Subject label for the associated subject
        :rtype: str
        """
        if self.subj_codes != {}:
            return self.subj_codes[code].get("english_name")
        return self.subj_code_english.get(code)

    def get_welsh_sbj_label(self, code: str) -> str:
        """
        Takes a subject code and returns the welsh name for the associated subject.

        :param code: Subject code to perform lookup with
        :type code: str
        :return: Subject label for the associated subject
        :rtype: str
        """
        if self.subj_codes != {}:
            return self.subj_codes[code].get("welsh_name")
        return self.subj_code_welsh.get(code)

    def get_json_list(self, raw_course_data: Dict[str, Any], get_key: Callable) -> List[Dict[str, Any]]:
        """
        Takes raw course data as a dictionary and a get_key function that returns the key associated with an element.
        Returns a list of JSON objects (as dicts) for this stats element.

        :param raw_course_data: Raw course data to use to generate JSON list
        :type raw_course_data: Dict[str, Any]
        :param get_key: Function to get the JSON key for each key in the XML
        :type get_key: Callable
        :return: List of JSON objects for each stats element in the raw course data
        :rtype: List[Dict[str, Any]]
        """

        json_elem_list = []
        raw_xml_list = self.get_raw_list(
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
        """
        Checks whether the passed XML element dictionary requires an unavailable reason.
        Returns True if an unavailable reason is required, otherwise False.

        :param xml_elem: XML element to check requirement
        :type xml_elem: Dict[str, Any]
        :return: True if an unavailable reason is required, otherwise False.
        :rtype: bool
        """
        if not self.has_data(xml_elem):
            return True

        unavail_reason_code = xml_elem.get(self.xml_unavail_reason_key)
        agg = xml_elem.get(self.xml_agg_key)
        agg_codes = self.get_aggs_for_code(unavail_reason_code)
        if agg in agg_codes:
            return True
        return False

    def get_aggs_for_code(self, unavail_reason_code: str) -> List[str]:
        """
        Returns the list of aggregation codes for the passed unavailable reason.

        :param unavail_reason_code: Unavailable reason code to get list of aggregation codes for.
        :type unavail_reason_code: str
        :return: List of corresponding aggregation codes
        :rtype: List[str]
        """
        return self.unavail_reason_english.get("data").get(unavail_reason_code).keys()

    def get_unavailable(self, elem: Dict[str, Any]) -> Union[Dict[str, Any], str]:
        """
        Takes an element and constructs a dictionary for unavailable reason data, and returns it.
        Returns an empty string if the aggregation code is "14".

        :param elem: Element to construct unavailable reason dictionary for
        :type elem: Dict[str, Any]
        :return: Dictionary of unavailable reason data, or an empty string if the aggregation code is "14"
        :rtype: Union[Dict[str, Any], str]
        """
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
    ) -> str:
        """
        Returns the unavailable reason string for the passed parameters.

        :param unavail_reason_code: Code for unavailable reason
        :type unavail_reason_code: str
        :param subj_key: Subject key for subject to be displayed with unavailable reason
        :type subj_key: str
        :param agg: Aggregation code
        :type agg: str
        :param xml_elem: XML element
        :type xml_elem: Dict[str, Any]
        :param resp_rate_state: Response rate
        :type resp_rate_state: str
        :param welsh: Defaults to False, function returns unavailable reason string in Welsh if set to True
        :type welsh: bool
        :return: Constructed unavailable reason string
        :rtype: str
        """
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

    def get_unavailable_reason_subj_english(self, sbj_key: str) -> str:
        """
        Takes a subject key and returns the english subject, or returns an appropriate unavailable reason if
        the subject key is empty.

        :param sbj_key: Key to return corresponding subject label for
        :type sbj_key: str
        :return: Corresponding english subject label, or an unavailable reason if the subject key doesn't exist
        :rtype: str
        """
        if sbj_key:
            return self.get_english_sbj_label(sbj_key)
        return self.unavail_reason_english["no-subject"]

    def get_unavailable_reason_subj_welsh(self, sbj_key: str) -> str:
        """
        Takes a subject key and returns the welsh subject, or returns an appropriate unavailable reason if
        the subject key is empty.

        :param sbj_key: Key to return corresponding subject label for
        :type sbj_key: str
        :return: Corresponding welsh subject label, or an unavailable reason if the subject key doesn't exist
        :rtype: str
        """
        if sbj_key:
            return self.get_welsh_sbj_label(sbj_key)
        return self.unavail_reason_welsh["no-subject"]

    @staticmethod
    def has_data(xml_elem: Dict[str, Any]) -> bool:
        """
        Returns True if the stats XML element has data otherwise False

        :param xml_elem: XML element as a dictionary
        :type xml_elem: Dict[str, Any]
        :return: True if the XML element has data else False
        :rtype: bool
        """
        return xml_elem is not None and len(xml_elem) > 1

    @staticmethod
    def get_json_value(xml_value: str) -> int:
        """
        Takes a value as a string and returns it as an integer

        :param xml_value: Value to return as an integer
        :type xml_value: str
        :return: XML value converted to an integer
        :rtype: int
        """
        if xml_value and xml_value.isdigit():
            xml_value = int(xml_value)
        return xml_value

    @staticmethod
    def get_raw_list(data: Dict[str, Any], element_key: str) -> Union[List[Dict[str, Any]], str]:
        """
        If value is a dict, return in list, otherwise return value

        :param data: Data to be processed and returned
        :type data: Dict[str, Any]
        :param element_key: Key to extract from data
        :type element_key: str
        :return: Value in a list if the value is a dict, otherwise only the value
        :rtype: Union[List[Dict[str, Any]], str]
        """
        if isinstance(data.get(element_key), dict):
            return [data.get(element_key)]
        return data.get(element_key)
