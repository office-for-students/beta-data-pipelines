"""Data transformation code for statistical data.

   Currently, we handle unexpected exceptions by letting them bubble up. This
   should help flush out problems during development and testing.

"""

from collections import OrderedDict
import datetime
import json
import logging
import pprint
import uuid
import os
import sys

import azure.cosmos.cosmos_client as cosmos_client
import xmltodict
import xml.etree.ElementTree as ET
import json

class CourseStats:

    def __init__(self):
        self.cwd = os.path.dirname(os.path.abspath(__file__))
        self.subj_code_english = self.read_lookup('subj_code_english.json')
        self.subj_code_welsh = self.read_lookup('subj_code_welsh.json')
        self.cont_unavail_reason = self.read_lookup('contunavailreason.json')

    def get_continuation_key(self,xml_key):
        return {
            'CONTUNAVAILREASON':'unavailable',
            "CONTPOP": 'number_of_students',
            "CONTAGG": 'aggregation_level',
            "CONTSBJ": 'subject',
            "UCONT": 'continuing_with_provider',
            "UDORMANT": 'dormant',
            "UGAINED": 'gained',
            "ULEFT": 'left',
            "ULOWER": 'lower',
        }[xml_key]

    def read_lookup(self, filename):
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

    def get_continuation_unavailable_reason(self, cont_elem):

        # TODO: find out where we get all the reasons from - spreadsheet not complete
        def get_reason(code, agg, has_data):
            if has_data:
                return self.cont_unavail_reason['data'][code].get(agg, "No info")
            return self.cont_unavail_reason['no-data'][code]

        unavailable_reason = {}
        code = cont_elem['CONTUNAVAILREASON']
        agg = cont_elem['CONTAGG']
        unavailable_reason['code'] = code
        unavailable_reason['reason'] = get_reason(code, agg, len(cont_elem) > 1)
        return unavailable_reason


    def get_continuation(self, raw_course_data):

        continuation_list = []

        # TODO Handle multiple continuation elements
        cont_elem = raw_course_data['CONTINUATION']
        continuation = {}
        for xml_key in cont_elem:
            json_key = self.get_continuation_key(xml_key)
            if json_key == 'subject':
                continuation[json_key] = self.get_continuation_subject(cont_elem)
            elif json_key == 'unavailable':
                continuation[json_key] = self.get_continuation_unavailable_reason(cont_elem)
            else:
                continuation[json_key] = cont_elem[xml_key]
            ordered_continuation = OrderedDict(sorted(continuation.items()))
        continuation_list.append(ordered_continuation)
        return continuation_list


    def build_stats(self, raw_course_data):
        stats = {}

        continuation = self.get_continuation(raw_course_data)
        stats['continuation'] = continuation
        return stats
