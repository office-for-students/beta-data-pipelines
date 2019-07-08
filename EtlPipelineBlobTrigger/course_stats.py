"""Data transformation code for statistical data."""

import json
import logging
import os
import unicodedata
from collections import OrderedDict


class Continuation:
    """Extracts and transforms the continuation course element"""

    def __init__(self):
        self.cwd = os.path.dirname(os.path.abspath(__file__))
        self.subj_code_english = self.get_lookup('subj_code_english')
        self.subj_code_welsh = self.get_lookup('subj_code_welsh')
        self.cont_unavail_reason = self.get_lookup('cont_unavail_reason')

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

    def get_lookup(self, lookup_name):
        filename_lookup = {
            'subj_code_english': 'subj_code_english.json',
            'subj_code_welsh': 'subj_code_welsh.json',
            'cont_unavail_reason': 'contunavailreason.json'
        }
        filename = filename_lookup[lookup_name]
        with open(os.path.join(self.cwd, f'lookup_files/{filename}')) as fp:
            return json.load(fp)

    def get_english_contsbj_label(self, code):
        return self.subj_code_english[code]

    def get_welsh_contsbj_label(self, code):
        return self.subj_code_welsh[code]

    def get_continuation_subject(self, cont_elem):
        subject = {}
        code = cont_elem['CONTSBJ']
        subject['code'] = code
        subject['english_label'] = self.get_english_contsbj_label(code)
        subject['welsh_label'] = self.get_welsh_contsbj_label(code)
        return subject

    def get_aggs_for_code(self, code):
        return self.cont_unavail_reason['data'][code].keys()

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

    def get_continuation_unavailable(self, cont_elem):

        def get_reason(code, agg, has_data, cont_elem):

            if not has_data:
                return self.cont_unavail_reason['no-data'][code]

            reason_str = self.cont_unavail_reason['data'][code].get(
                agg, "No info")
            reason_str = unicodedata.normalize("NFKD", reason_str)
            if reason_str == "No info":
                logging.error(
                    "Unexpected error unable to get valid reason string for {cont_elem}"
                )
                return reason_str

            subj_code = cont_elem.get('CONTSBJ')
            if subj_code:
                subj_name = self.get_english_contsbj_label(subj_code)
            else:
                subj_name = 'this subject'
            return reason_str + subj_name + '.'

        unavailable_reason = {}
        has_data = len(cont_elem) > 1
        code = cont_elem['CONTUNAVAILREASON']
        agg = cont_elem['CONTAGG'] if has_data else None
        unavailable_reason['code'] = code
        unavailable_reason['reason'] = get_reason(code, agg, has_data,
                                                  cont_elem)
        return unavailable_reason

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
                    continuation[json_key] = self.get_continuation_subject(
                        cont_elem)
                elif json_key == 'unavailable':
                    if self.need_unavail_element(cont_elem):
                        continuation[
                            json_key] = self.get_continuation_unavailable(
                                cont_elem)
                else:
                    continuation[json_key] = cont_elem[xml_key]
                ordered_continuation = OrderedDict(sorted(
                    continuation.items()))
            continuation_list.append(ordered_continuation)
        return continuation_list


class CourseStats:
    def get_stats(self, raw_course_data):
        stats = {}

        continuation = Continuation()
        stats['continuation'] = continuation.get_continuation(raw_course_data)
        return stats
