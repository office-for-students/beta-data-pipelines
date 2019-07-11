"""Data transformation code for statistical data."""

import json
import inspect
import os
import sys
import unicodedata
from collections import OrderedDict

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, CURRENTDIR)

from validators import validate_unavailable_reason_code, validate_agg


class CourseStats:
    def get_stats(self, raw_course_data):
        stats = {}

        continuation = Continuation()
        employment = Employment()
        stats['continuation'] = continuation.get_continuation(raw_course_data)
        stats['employment'] = employment.get_employment(raw_course_data)
        return stats

class Continuation:
    """Extracts and transforms the continuation course element"""

    def __init__(self):
        xml_element_key = 'CONTINUATION'
        xml_subj_key = 'CONTSBJ'
        xml_agg_key = 'CONTAGG'
        xml_unavail_reason_key = 'CONTUNAVAILREASON'

        self.shared_utils = SharedUtils(xml_element_key, xml_subj_key, xml_agg_key, xml_unavail_reason_key)

    def get_continuation_key(self, xml_key):
        return {
            'CONTUNAVAILREASON': 'unavailable',
            "CONTPOP": 'number_of_students',
            "CONTAGG": 'aggregation_level',
            "CONTSBJ": 'subject',
            "UCONT": 'continuing_with_provider',
            "UDORMANT": 'dormant',
            "UGAINED": 'gained',
            "ULEFT": 'left',
            "ULOWER": 'lower',
        }[xml_key]

        return self.shared_utils.unavail_reason['data'][code].keys()

    def get_cont_subject(self, cont_elem, subj_key):
        return self.shared_utils.get_subject(cont_elem)


    def get_continuation(self, raw_course_data):
        def get_raw_continuation_list(raw_course_data):
            if isinstance(raw_course_data['CONTINUATION'], dict):
                return [raw_course_data['CONTINUATION']]
            return raw_course_data['CONTINUATION']

        continuation_list = []
        raw_continuation_list = get_raw_continuation_list(raw_course_data)
        for cont_elem in raw_continuation_list:
            continuation = {}
            for xml_key in cont_elem:
                json_key = self.get_continuation_key(xml_key)
                if json_key == 'subject':
                    continuation[json_key] = self.get_cont_subject(
                        cont_elem, 'CONTSBJ')
                elif json_key == 'unavailable':
                    if self.shared_utils.need_unavailable(
                            cont_elem):
                        continuation[json_key] = self.shared_utils.get_unavailable( cont_elem)
                else:
                    continuation[json_key] = cont_elem[xml_key]
                ordered_continuation = OrderedDict(sorted(
                    continuation.items()))
            continuation_list.append(ordered_continuation)
        return continuation_list

class Employment:
    """Extracts and transforms the Employment course element"""

    def __init__(self):
        xml_element_key = 'EMPLOYMENT'
        xml_subj_key = 'EMPSBJ'
        xml_agg_key = 'EMPAGG'
        xml_unavail_reason_key = 'EMPUNAVAILREASON'

        self.shared_utils = SharedUtils(xml_element_key, xml_subj_key, xml_agg_key, xml_unavail_reason_key)

    def get_key(self, xml_key):
        return {
            "EMPUNAVAILREASON": "unavailable",
            'EMPPOP': 'number_of_students',
            'EMPRESP_RATE': 'response_rate',
            "EMPAGG": "aggregation_level",
            "EMPSBJ": "subject",
            'WORKSTUDY': 'in_work_or_study',
            "STUDY": 'in_study',
            "ASSUNEMP": 'assumed_to_be_unemeployed',
            "BOTH": 'in_work_and_study',
            "NOAVAIL": 'not_available_for_work_or_study',
            "WORK": 'in_work',
        }[xml_key]


    def get_employment(self, raw_course_data):
        subj_key = 'EMPSBJ'
        agg_key = 'EMPAGG'
        unavail_reason_key = 'EMPUNAVAILREASON'
        element_key = 'EMPLOYMENT'

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, element_key)
        for xml_elem in raw_xml_list:
            json_elem = {}
            for xml_key in xml_elem:
                json_key = self.get_key(xml_key)
                if json_key == 'subject':
                    json_elem[json_key] = self.shared_utils.get_subject(
                        xml_elem)
                elif json_key == 'unavailable':
                    if self.shared_utils.need_unavailable(
                            xml_elem):
                        json_elem[
                            json_key] = self.shared_utils.get_unavailable(xml_elem)
                else:
                    json_elem[json_key] = xml_elem[xml_key]
                ordered_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(ordered_json_elem)
        return json_elem_list

class SharedUtils:
    """Functionality required by several stats related classes"""

    def __init__(self, xml_element_key, xml_subj_key, xml_agg_key, xml_unavail_reason_key):

        self.xml_element_key = xml_element_key
        self.xml_subj_key = xml_subj_key
        self.xml_agg_key = xml_agg_key
        self.xml_unavail_reason_key = xml_unavail_reason_key

        self.cwd = os.path.dirname(os.path.abspath(__file__))
        self.subj_code_english = self.get_lookup('subj_code_english')
        self.subj_code_welsh = self.get_lookup('subj_code_welsh')
        self.unavail_reason = self.get_lookup('unavail_reason')

    def get_lookup(self, lookup_name):
        filename = {
            'subj_code_english': 'subj_code_english.json',
            'subj_code_welsh': 'subj_code_welsh.json',
            'unavail_reason': 'contunavailreason.json'
        }[lookup_name]
        with open(os.path.join(self.cwd, f'lookup_files/{filename}')) as fp:
            return json.load(fp)

    def get_english_sbj_label(self, code):
        return self.subj_code_english[code]

    def get_welsh_sbj_label(self, code):
        return self.subj_code_welsh[code]

    def get_subject(self, xml_elem):
        subj_key = xml_elem[self.xml_subj_key]
        subject = {}
        subject['code'] = subj_key
        subject['english_label'] = self.get_english_sbj_label(subj_key)
        subject['welsh_label'] = self.get_welsh_sbj_label(subj_key)
        return subject

    def get_aggs_for_code(self, unavail_reason_code):
        return self.unavail_reason['data'][unavail_reason_code].keys()

    def need_unavailable(self, xml_elem):
        has_data = len(xml_elem) > 1
        if not has_data:
            return True

        unavail_reason_code = xml_elem[self.xml_unavail_reason_key]
        agg = xml_elem[self.xml_agg_key]
        agg_codes = self.get_aggs_for_code(unavail_reason_code)
        if agg in agg_codes:
            return True
        return False

    def get_unavailable_reason_subj(self, sbj_key):
        if sbj_key:
            return self.get_english_sbj_label(sbj_key)
        return 'this subject'

    def get_unavailable_reason_str(self, unavail_reason_code, subj_key, agg,
                                   xml_elem):
        validate_unavailable_reason_code(unavail_reason_code)

        has_data = len(xml_elem) > 1
        if not has_data:
            return self.unavail_reason['no-data'][unavail_reason_code]

        validate_agg(unavail_reason_code, has_data, agg, self.unavail_reason)
        partial_reason_str = self.unavail_reason['data'][unavail_reason_code][
            agg]
        partial_reason_str = unicodedata.normalize("NFKD", partial_reason_str)
        subj = self.get_unavailable_reason_subj(subj_key)
        return partial_reason_str + subj + '.'

    def get_unavailable(self, elem):
        unavailable = {}
        has_data = len(elem) > 1
        subj_key = elem.get(self.xml_subj_key)
        agg = elem[self.xml_agg_key] if has_data else None
        unavail_reason_code = elem[self.xml_unavail_reason_key]
        validate_unavailable_reason_code(unavail_reason_code)

        unavailable['code'] = unavail_reason_code
        unavailable['reason'] = self.get_unavailable_reason_str(
            unavail_reason_code, subj_key, agg, elem)
        return unavailable

    @staticmethod
    def get_raw_list(raw_course_data, element_key):
        """Get a list for the element"""

        if isinstance(raw_course_data[element_key], dict):
            return [raw_course_data[element_key]]
        return raw_course_data[element_key]


