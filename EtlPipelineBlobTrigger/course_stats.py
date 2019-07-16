"""Data transformation code for statistical data."""

import inspect
import json
import os
import sys
import unicodedata
from collections import OrderedDict

from validators import validate_agg, validate_unavailable_reason_code

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, CURRENTDIR)


class CourseStats:
    def get_stats(self, raw_course_data):
        stats = {}

        continuation = Continuation()
        employment = Employment()
        entry = Entry()
        job_type = JobType()
        nss = Nss()
        salary = Salary()

        stats['continuation'] = continuation.get_stats(raw_course_data)
        stats['employment'] = employment.get_stats(raw_course_data)
        stats['entry'] = entry.get_stats(raw_course_data)
        stats['job_type'] = job_type.get_stats(raw_course_data)
        stats['nss'] = nss.get_stats(raw_course_data)
        stats['salary'] = salary.get_stats(raw_course_data)
        return stats


class Continuation:
    """Extracts and transforms the Continuation course element"""

    def __init__(self):
        self.xml_element_key = 'CONTINUATION'
        self.xml_subj_key = 'CONTSBJ'
        self.xml_agg_key = 'CONTAGG'
        self.xml_unavail_reason_key = 'CONTUNAVAILREASON'

        self.shared_utils = SharedUtils(self.xml_element_key,
                                        self.xml_subj_key, self.xml_agg_key,
                                        self.xml_unavail_reason_key)

    def get_key(self, xml_key):
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

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class Employment:
    """Extracts and transforms the Employment course element"""

    def __init__(self):
        self.xml_element_key = 'EMPLOYMENT'
        self.xml_subj_key = 'EMPSBJ'
        self.xml_agg_key = 'EMPAGG'
        self.xml_unavail_reason_key = 'EMPUNAVAILREASON'

        self.shared_utils = SharedUtils(self.xml_element_key,
                                        self.xml_subj_key, self.xml_agg_key,
                                        self.xml_unavail_reason_key)

    def get_key(self, xml_key):
        return {
            "EMPUNAVAILREASON": "unavailable",
            'EMPPOP': 'number_of_students',
            'EMPRESP_RATE': 'response_rate',
            "EMPAGG": "aggregation_level",
            "EMPSBJ": "subject",
            'WORKSTUDY': 'in_work_or_study',
            "STUDY": 'in_study',
            "ASSUNEMP": 'assumed_to_be_unemployed',
            "BOTH": 'in_work_and_study',
            "NOAVAIL": 'not_available_for_work_or_study',
            "WORK": 'in_work',
        }[xml_key]

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class Entry:
    """Extracts and transforms the Entry course element"""

    def __init__(self):
        self.xml_element_key = 'ENTRY'
        self.xml_subj_key = 'ENTSBJ'
        self.xml_agg_key = 'ENTAGG'
        self.xml_unavail_reason_key = 'ENTUNAVAILREASON'

        self.shared_utils = SharedUtils(self.xml_element_key,
                                        self.xml_subj_key, self.xml_agg_key,
                                        self.xml_unavail_reason_key)

    def get_key(self, xml_key):
        return {
            'ENTUNAVAILREASON': 'unavailable',
            "ENTPOP": 'number_of_students',
            "ENTAGG": 'aggregation_level',
            "ENTSBJ": 'subject',
            "ACCESS": 'access',
            "ALEVEL": 'a-level',
            "BACC": 'baccalaureate',
            "DEGREE": 'degree',
            "FOUNDTN": 'foundation',
            "NOQUALS": 'none',
            "OTHER": 'other_qualifications',
            "OTHERHE": 'another_higher_education_qualifications',
        }[xml_key]

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class JobType:
    """Extracts and transforms the JobType course element"""

    def __init__(self):
        self.xml_element_key = 'JOBTYPE'
        self.xml_subj_key = 'JOBSBJ'
        self.xml_agg_key = 'JOBAGG'
        self.xml_unavail_reason_key = 'JOBUNAVAILREASON'

        self.shared_utils = SharedUtils(self.xml_element_key,
                                        self.xml_subj_key, self.xml_agg_key,
                                        self.xml_unavail_reason_key)

    def get_key(self, xml_key):
        return {
            'JOBUNAVAILREASON': 'unavailable',
            "JOBPOP": 'number_of_students',
            "JOBAGG": 'aggregation_level',
            "JOBSBJ": 'subject',
            'JOBRESP_RATE': 'resp_rate',
            "PROFMAN": 'professional_or_managerial_jobs',
            "OTHERJOB": 'non_professional_or_managerial_jobs',
            "UNKWN": 'unknown_professions',
        }[xml_key]

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class Nss:
    """Extracts and transforms the NSS course element"""

    def __init__(self):
        self.xml_element_key = 'NSS'
        self.xml_subj_key = 'NSSSBJ'
        self.xml_agg_key = 'NSSAGG'
        self.xml_unavail_reason_key = 'NSSUNAVAILREASON'

        self.shared_utils = SharedUtils(self.xml_element_key,
                                        self.xml_subj_key, self.xml_agg_key,
                                        self.xml_unavail_reason_key)
        self.question_lookup = self.shared_utils.get_lookup('nss_question_number')
        self.q_number_string_lookup = {f'Q{i}':f'question_{i}' for i in range(1, 28)}
        self.nss_key_lookup_table = self.get_nss_key_lookup_table()

    def get_nss_key_lookup_table(self):
        lookup = {
            'NSSUNAVAILREASON': 'unavailable',
            "NSSPOP": 'number_of_students',
            "NSSAGG": 'aggregation_level',
            "NSSSBJ": 'subject',
            'NSSRESP_RATE': 'resp_rate',
        }
        lookup.update(self.q_number_string_lookup)
        return lookup


    def get_key(self, xml_key):
        return self.nss_key_lookup_table[xml_key]

    def get_stats(self, raw_course_data):
        return self.get_json_list(raw_course_data, self.get_key)

    def is_question(self, xml_key):
        return xml_key in self.q_number_string_lookup

    def get_question(self, xml_elem, xml_key):
        question = {}

        question['description'] = self.question_lookup[xml_key]
        question['agree_or_strongly_agree'] = xml_elem[xml_key]
        return question


    def get_json_list(self, raw_course_data, get_key):
        """Returns a list of JSON objects (as dicts) for the Statistics element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data,
                                                self.xml_element_key)
        for xml_elem in raw_xml_list:

            json_elem = {}
            for xml_key in xml_elem:
                json_key = get_key(xml_key)
                if self.is_question(xml_key):
                    json_elem[json_key] = self.get_question(xml_elem, xml_key)
                elif json_key == 'subject':
                    json_elem[json_key] = self.shared_utils.get_subject(xml_elem)
                elif json_key == 'unavailable':
                    if self.shared_utils.need_unavailable(xml_elem):
                        json_elem[json_key] = self.shared_utils.get_unavailable(xml_elem)
                else:
                    json_elem[json_key] = self.shared_utils.get_json_value(
                        xml_elem[xml_key])
                #sorted_json_elem = OrderedDict(sorted(json_elem.items(), key=lambda t:get_key(t[0])))
            json_elem_list.append(json_elem)
        return json_elem_list


class Salary:
    """Extracts and transforms the Salary course element"""

    # TODO add additional fields when remaining mappings available

    def __init__(self):
        self.xml_element_key = 'SALARY'
        self.xml_subj_key = 'SALSBJ'
        self.xml_agg_key = 'SALAGG'
        self.xml_unavail_reason_key = 'SALUNAVAILREASON'

        self.shared_utils = SharedUtils(self.xml_element_key,
                                        self.xml_subj_key, self.xml_agg_key,
                                        self.xml_unavail_reason_key)

    def get_key(self, xml_key):
        # TODO add additional fields once mappings are completed
        # and change back to }[xml_key] so we'll bubble up any KeyErrors.
        return {
            "SALUNAVAILREASON": "unavailable",
            'SALPOP': 'number_of_graduates',
            'SALRESP_RATE': 'response_rate',
            "SALAGG": "aggregation_level",
            "SALSBJ": 'subject',
            "UQ": "higher_quartile",
            'LQ': 'lower_quartile',
            "MEDIAN": 'median',
        }.get(xml_key)

    def get_stats(self, raw_course_data):
        # TODO - use shared version when all Salary fields mapped
        return self.get_json_list(raw_course_data)

    def get_json_list(self, raw_course_data):
        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data,
                                                self.xml_element_key)
        for xml_elem in raw_xml_list:
            json_elem = {}
            for xml_key in xml_elem:
                json_key = self.get_key(xml_key)
                if not json_key:
                    continue
                if json_key == 'subject':
                    json_elem[json_key] = self.shared_utils.get_subject(
                        xml_elem)
                elif json_key == 'unavailable':
                    if self.shared_utils.need_unavailable(xml_elem):
                        json_elem[
                            json_key] = self.shared_utils.get_unavailable(
                                xml_elem)
                else:
                    json_elem[json_key] = self.shared_utils.get_json_value(
                        xml_elem[xml_key])
                ordered_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(ordered_json_elem)
        return json_elem_list


class SharedUtils:
    """Functionality required by several stats related classes"""

    def __init__(self, xml_element_key, xml_subj_key, xml_agg_key,
                 xml_unavail_reason_key):

        self.xml_element_key = xml_element_key
        self.xml_subj_key = xml_subj_key
        self.xml_agg_key = xml_agg_key
        self.xml_unavail_reason_key = xml_unavail_reason_key
        self.subj_code_english = self.get_lookup('subj_code_english')
        self.subj_code_welsh = self.get_lookup('subj_code_welsh')
        self.unavail_reason = self.get_lookup('unavail_reason')

    def get_lookup(self, lookup_name):
        cwd = os.path.dirname(os.path.abspath(__file__))
        filename = {
            'subj_code_english': 'subj_code_english.json',
            'subj_code_welsh': 'subj_code_welsh.json',
            'unavail_reason': 'unavailreason.json',
            'nss_question_number': 'nss_question_number.json'
        }[lookup_name]
        with open(os.path.join(cwd, f'lookup_files/{filename}')) as infile:
            return json.load(infile)

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

    def has_data(self, xml_elem):
        """Returns True if the statistical XML element has data otherwise False"""
        return len(xml_elem) > 1

    def need_unavailable(self, xml_elem):
        """Returns True if we need to include an unavailable object otherwise False"""
        if not self.has_data(xml_elem):
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

        if not self.has_data(xml_elem):
            reason_str = self.unavail_reason['no-data'][unavail_reason_code]
            return unicodedata.normalize("NFKD", reason_str)

        validate_agg(unavail_reason_code, self.has_data(xml_elem), agg,
                     self.unavail_reason)
        partial_reason_str = self.unavail_reason['data'][unavail_reason_code][
            agg]
        partial_reason_str = unicodedata.normalize("NFKD", partial_reason_str)
        subj = self.get_unavailable_reason_subj(subj_key)
        return partial_reason_str + subj + '.'

    def get_unavailable(self, elem):
        unavailable = {}
        subj_key = elem.get(self.xml_subj_key)
        agg = elem[self.xml_agg_key] if self.has_data(elem) else None
        unavail_reason_code = elem[self.xml_unavail_reason_key]
        validate_unavailable_reason_code(unavail_reason_code)

        unavailable['code'] = unavail_reason_code
        unavailable['reason'] = self.get_unavailable_reason_str(
            unavail_reason_code, subj_key, agg, elem)
        return unavailable

    def get_json_value(self, xml_value):
        if xml_value.isdigit():
            xml_value = int(xml_value)
        return xml_value

    def get_json_list(self, raw_course_data, get_key):
        """Returns a list of JSON objects (as dicts) for the Statistics element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data,
                                                self.xml_element_key)
        for xml_elem in raw_xml_list:

            json_elem = {}
            for xml_key in xml_elem:
                json_key = get_key(xml_key)
                if json_key == 'subject':
                    json_elem[json_key] = self.get_subject(xml_elem)
                elif json_key == 'unavailable':
                    if self.need_unavailable(xml_elem):
                        json_elem[json_key] = self.get_unavailable(xml_elem)
                else:
                    json_elem[json_key] = self.get_json_value(
                        xml_elem[xml_key])
                ordered_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(ordered_json_elem)
        return json_elem_list

    @staticmethod
    def get_raw_list(raw_course_data, element_key):
        """Get a list for the element"""

        if isinstance(raw_course_data[element_key], dict):
            return [raw_course_data[element_key]]
        return raw_course_data[element_key]
