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
        stats['continuation'] = continuation.get_continuation(raw_course_data)
        return stats

class SharedUtils:
    """Functionality required by several stats related classes"""

    def __init__(self):
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

    def get_subject(self, elem, raw_subj_key):
        subj_key = elem[raw_subj_key]
        subject = {}
        subject['code'] = subj_key
        subject['english_label'] = self.get_english_sbj_label(subj_key)
        subject['welsh_label'] = self.get_welsh_sbj_label(subj_key)
        return subject

    def get_aggs_for_code(self, unavail_reason_code):
        return self.cont_unavail_reason['data'][unavail_reason_code].keys()

    def get_unavailable_reason_subj(self, sbj_key):
        if sbj_key:
            return self.get_english_sbj_label(sbj_key)
        return 'this subject'

    def need_unavailable(self, cont_elem, unavailreason_key):
        has_data = len(cont_elem) > 1
        if not has_data:
            return True

        code = cont_elem['CONTUNAVAILREASON']
        agg = cont_elem['CONTAGG']
        agg_codes = self.get_aggs_for_code(code)
        if agg in agg_codes:
            return True
        return False

    def get_unavailable_reason_str(self, unavail_reason_code, subj_key, agg,
                                   elem):
        validate_unavailable_reason_code(unavail_reason_code)

        has_data = len(elem) > 1
        if not has_data:
            return self.unavail_reason['no-data'][code]

        validate_agg(unavail_reason_code, has_data, agg, self.unavail_reason)
        partial_reason_str = self.unavail_reason['data'][unavail_reason_code][
            agg]
        partial_reason_str = unicodedata.normalize("NFKD", partial_reason_str)
        subj = self.get_unavailable_reason_subj(subj_key)
        return partial_reason_str + subj + '.'

    def get_unavailable(self, cont_elem, raw_subj_key, raw_agg_key,
                        raw_unavail_reason_key):
        unavailable = {}
        has_data = len(cont_elem) > 1
        subj_key = cont_elem.get(raw_subj_key)
        agg = cont_elem[raw_agg_key] if has_data else None
        unavail_reason_code = cont_elem[raw_unavail_reason_key]
        validate_unavailable_reason_code(unavail_reason_code)

        unavailable['code'] = unavail_reason_code
        unavailable['reason'] = self.get_unavailable_reason_str(
            unavail_reason_code, subj_key, agg, cont_elem)
        return unavailable


class Continuation:
    """Extracts and transforms the continuation course element"""

    def __init__(self):
        self.shared_utils = SharedUtils()

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

    def get_aggs_for_code(self, code):
        return self.shared_utils.unavail_reason['data'][code].keys()

    def need_unavail_element(self, cont_elem):
        has_data = len(cont_elem) > 1
        if not has_data:
            return True

        code = cont_elem['CONTUNAVAILREASON']
        agg = cont_elem['CONTAGG']
        agg_codes = self.get_aggs_for_code(code)
        if agg in agg_codes:
            return True
        return False

    def get_cont_subject(self, cont_elem, subj_key):
        return self.shared_utils.get_subject(cont_elem, subj_key)

    def get_cont_unavailable(self, cont_elem):
        subj_key = 'CONTSBJ'
        agg_key = 'CONTAGG'
        unavail_reason_key = 'CONTUNAVAILREASON'
        return self.shared_utils.get_unavailable(cont_elem, subj_key, agg_key,
                                                 unavail_reason_key)

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
                    if self.need_unavail_element(cont_elem):
                        continuation[json_key] = self.get_cont_unavailable(
                            cont_elem)
                else:
                    continuation[json_key] = cont_elem[xml_key]
                ordered_continuation = OrderedDict(sorted(
                    continuation.items()))
            continuation_list.append(ordered_continuation)
        return continuation_list
