"""Data extraction and transformation for statistical data."""

import json
import os
import unicodedata
from collections import OrderedDict

from validators import (
    validate_agg,
    validate_unavailable_reason_code,
    validate_leo_unavailable_reason_code,
    validate_leo_element_with_data,
)


def get_stats(raw_course_data, country_code):

    continuation = Continuation()
    employment = Employment()
    entry = Entry()
    job_type = JobType()
    job_list = JobList()
    leo = Leo(country_code)
    nhs_nss = NhsNss()
    nss = Nss()
    salary = Salary()
    tariff = Tariff()

    stats = {}
    stats["continuation"] = continuation.get_stats(raw_course_data)
    stats["employment"] = employment.get_stats(raw_course_data)
    stats["entry"] = entry.get_stats(raw_course_data)
    stats["job_type"] = job_type.get_stats(raw_course_data)
    stats["job_list"] = job_list.get_stats(raw_course_data)
    stats["leo"] = leo.get_stats(raw_course_data)
    stats["nhs_nss"] = nhs_nss.get_stats(raw_course_data)
    stats["nss"] = nss.get_stats(raw_course_data)
    stats["salary"] = salary.get_stats(raw_course_data)
    stats["tariff"] = tariff.get_stats(raw_course_data)
    return stats


class Continuation:
    """Extracts and transforms the Continuation course element"""

    def __init__(self):
        self.xml_element_key = "CONTINUATION"
        self.xml_subj_key = "CONTSBJ"
        self.xml_agg_key = "CONTAGG"
        self.xml_unavail_reason_key = "CONTUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key):
        return {
            "CONTUNAVAILREASON": "unavailable",
            "CONTPOP": "number_of_students",
            "CONTAGG": "aggregation_level",
            "CONTSBJ": "subject",
            "UCONT": "continuing_with_provider",
            "UDORMANT": "dormant",
            "UGAINED": "gained",
            "ULEFT": "left",
            "ULOWER": "lower",
        }[xml_key]

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class Employment:
    """Extracts and transforms the Employment course element"""

    def __init__(self):
        self.xml_element_key = "EMPLOYMENT"
        self.xml_subj_key = "EMPSBJ"
        self.xml_agg_key = "EMPAGG"
        self.xml_unavail_reason_key = "EMPUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key):
        return {
            "EMPUNAVAILREASON": "unavailable",
            "EMPPOP": "number_of_students",
            "EMPRESP_RATE": "response_rate",
            "EMPAGG": "aggregation_level",
            "EMPSBJ": "subject",
            "WORKSTUDY": "in_work_or_study",
            "STUDY": "in_study",
            "ASSUNEMP": "assumed_to_be_unemployed",
            "BOTH": "in_work_and_study",
            "NOAVAIL": "not_available_for_work_or_study",
            "WORK": "in_work",
        }[xml_key]

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class Entry:
    """Extracts and transforms the Entry course element"""

    def __init__(self):
        self.xml_element_key = "ENTRY"
        self.xml_subj_key = "ENTSBJ"
        self.xml_agg_key = "ENTAGG"
        self.xml_unavail_reason_key = "ENTUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key):
        return {
            "ENTUNAVAILREASON": "unavailable",
            "ENTPOP": "number_of_students",
            "ENTAGG": "aggregation_level",
            "ENTSBJ": "subject",
            "ACCESS": "access",
            "ALEVEL": "a-level",
            "BACC": "baccalaureate",
            "DEGREE": "degree",
            "FOUNDTN": "foundation",
            "NOQUALS": "none",
            "OTHER": "other_qualifications",
            "OTHERHE": "another_higher_education_qualifications",
        }[xml_key]

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class JobType:
    """Extracts and transforms the JobType course element"""

    def __init__(self):
        self.xml_element_key = "JOBTYPE"
        self.xml_subj_key = "JOBSBJ"
        self.xml_agg_key = "JOBAGG"
        self.xml_unavail_reason_key = "JOBUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

    @staticmethod
    def get_key(xml_key):
        return {
            "JOBUNAVAILREASON": "unavailable",
            "JOBPOP": "number_of_students",
            "JOBAGG": "aggregation_level",
            "JOBSBJ": "subject",
            "JOBRESP_RATE": "response_rate",
            "PROFMAN": "professional_or_managerial_jobs",
            "OTHERJOB": "non_professional_or_managerial_jobs",
            "UNKWN": "unknown_professions",
        }[xml_key]

    def get_stats(self, raw_course_data):
        return self.shared_utils.get_json_list(raw_course_data, self.get_key)


class JobList:
    """Extracts and transforms the COMMON entries in a KISCOURSE"""

    """
    The following is the agreed structure that COMMON elements in a KISCOURSE
    element will be transformed to:

    "job_list": [{
        "aggregation_level": "integer",
        "list": [{
            "job": "string",
            "percentage_of_students": "integer",
            "order": "integer",
        }],
        "number_of_students": "integer",
        "response_rate": "integer",
        "subject": {
            "code": "string",
            "english_label": "string",
            "welsh_label": "string"
        },
        "unavailable": {
            "code": "integer",
            "reason": "string"
        }
    }]

    """

    def __init__(self):
        self.xml_element_key = "COMMON"
        self.xml_subj_key = "COMSBJ"
        self.xml_agg_key = "COMAGG"
        self.xml_unavail_reason_key = "COMUNAVAILREASON"
        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )
        self.data_fields_lookup = self.shared_utils.get_lookup("common_data_fields")

    def get_stats(self, raw_course_data):
        """Extracts and transforms the COMMON entries in a KISCOURSE"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, self.xml_element_key)
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(xml_elem)
            json_elem_list.append(json_elem)
        return json_elem_list

    def get_json_data(self, xml_elem):
        """Extracts and transforms a COMMON entry with data in a KISCOURSE"""

        lookup = self.data_fields_lookup
        json_data = {}
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                json_data[lookup[xml_key][0]] = self.shared_utils.get_json_value(
                    xml_elem[xml_key]
                )
            else:
                if xml_key in xml_elem:
                    json_key = lookup[xml_key][0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(xml_elem)
                    elif json_key == "list":
                        json_data["list"] = self.get_list_field(xml_elem)
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_list_field(self, xml_elem):
        """Extracts and transforms the JOBLIST entries in a COMMON element"""

        list_field = []
        job_lists = self.shared_utils.get_raw_list(xml_elem, "JOBLIST")
        for job_list in job_lists:
            job_list_item = {}
            job_list_item["job"] = job_list["JOB"]
            job_list_item["percentage_of_students"] = job_list["PERC"]
            job_list_item["order"] = job_list["ORDER"]
            list_field.append(job_list_item)
        return list_field


class Leo:
    """Extracts and transforms the LEO course element"""

    # Current understanding of how to transform this data is as follows:
    #    If there is data:
    #       no unavailable section needed

    #    What follows is when there is data
    #
    #    If not in England:
    #        return the appropriate message
    #    elif reason code 1:
    #        return the appropriate message
    #    elif reason code 2:
    #        return the appropriate message
    #    else error as we do not expect another reason code
    #
    def __init__(self, course_code):
        self.xml_element_key = "LEO"
        self.xml_unavail_reason_key = "LEOUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key, "LEOSBJ", "LEOAGG", self.xml_unavail_reason_key
        )

        self.country_code = course_code
        self.unavail_reason = self.shared_utils.get_lookup("leo_unavail_reason")
        self.data_fields_lookup = self.shared_utils.get_lookup("leo_data_fields")

    def need_unavailable(self, xml_elem):
        """Returns True if unavailable is needed otherwise False"""

        if self.shared_utils.has_data(xml_elem):
            validate_leo_element_with_data(xml_elem, self.country_code)
            return False
        return True

    def course_outside_england(self):
        return self.country_code != "XF"

    def get_unavailable_reason_str(self, unavail_reason_code):
        if self.course_outside_england():
            reason_str = self.unavail_reason["outside_england"]
        else:
            reason_str = self.unavail_reason[unavail_reason_code]
        return unicodedata.normalize("NFKD", reason_str)

    def get_unavailable(self, xml_elem):
        unavailable = {}
        unavail_reason_code = xml_elem[self.xml_unavail_reason_key]
        validate_leo_unavailable_reason_code(unavail_reason_code)

        unavailable["code"] = int(unavail_reason_code)
        unavailable["reason"] = self.get_unavailable_reason_str(unavail_reason_code)
        return unavailable

    def get_json_data(self, xml_elem):
        """Extracts and formats the data from the XML element"""

        lookup = self.data_fields_lookup
        json_data = {}
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                json_data[lookup[xml_key][0]] = self.shared_utils.get_json_value(
                    xml_elem[xml_key]
                )
            else:
                if xml_key in xml_elem:
                    json_key = lookup[xml_key][0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(xml_elem)
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_stats(self, raw_course_data):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, self.xml_element_key)
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.get_unavailable(xml_elem)
            sorted_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(sorted_json_elem)
        return json_elem_list


class Nss:
    """Extracts and transforms the NSS course element"""

    NUM_QUESTIONS = 27

    def __init__(self):
        self.xml_element_key = "NSS"

        self.shared_utils = SharedUtils(
            self.xml_element_key, "NSSSBJ", "NSSAGG", "NSSUNAVAILREASON"
        )
        self.question_lookup = self.shared_utils.get_lookup("nss_question_description")
        self.nss_data_fields_lookup = self.shared_utils.get_lookup("nss_data_fields")
        self.is_question_lookup = [f"Q{i}" for i in range(1, Nss.NUM_QUESTIONS + 1)]

    def is_question(self, xml_key):
        return xml_key in self.is_question_lookup

    def get_question(self, xml_elem, xml_key):
        question = {}
        question["description"] = self.question_lookup[xml_key]
        question["agree_or_strongly_agree"] = int(xml_elem[xml_key])
        return question

    def get_mandatory_field(self, xml_elem, xml_key):
        if self.is_question(xml_key):
            return self.get_question(xml_elem, xml_key)
        return self.shared_utils.get_json_value(xml_elem[xml_key])

    def get_json_data(self, xml_elem):
        lookup = self.nss_data_fields_lookup
        json_data = OrderedDict()
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                json_data[lookup[xml_key][0]] = self.get_mandatory_field(
                    xml_elem, xml_key
                )
            else:
                if xml_key in xml_elem:
                    json_key = lookup[xml_key][0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(xml_elem)
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_stats(self, raw_course_data):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, self.xml_element_key)
        for xml_elem in raw_xml_list:
            json_elem = OrderedDict()
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(xml_elem)
            json_elem_list.append(json_elem)
        return json_elem_list


class NhsNss:
    """Extracts and transforms the NHS NSS course element"""

    NUM_QUESTIONS = 6

    def __init__(self):
        self.xml_element_key = "NHSNSS"

        self.shared_utils = SharedUtils(
            self.xml_element_key, "NHSSBJ", "NHSAGG", "NHSUNAVAILREASON"
        )
        self.question_description_lookup = self.shared_utils.get_lookup(
            "nhs_question_description"
        )
        self.data_fields_lookup = self.shared_utils.get_lookup("nhs_data_fields")
        self.is_question_lookup = [
            f"NHSQ{i}" for i in range(1, NhsNss.NUM_QUESTIONS + 1)
        ]

    def is_question(self, xml_key):
        return xml_key in self.is_question_lookup

    def get_question(self, xml_elem, xml_key):
        question = {}
        question["description"] = self.question_description_lookup[xml_key]
        question["agree_or_strongly_agree"] = int(xml_elem[xml_key])
        return question

    def get_mandatory_field(self, xml_elem, xml_key):
        if self.is_question(xml_key):
            return self.get_question(xml_elem, xml_key)
        return self.shared_utils.get_json_value(xml_elem[xml_key])

    def get_json_data(self, xml_elem):
        lookup = self.data_fields_lookup
        json_data = OrderedDict()
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                json_data[lookup[xml_key][0]] = self.get_mandatory_field(
                    xml_elem, xml_key
                )
            else:
                if xml_key in xml_elem:
                    json_key = lookup[xml_key][0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(xml_elem)
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_stats(self, raw_course_data):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, self.xml_element_key)
        for xml_elem in raw_xml_list:
            json_elem = OrderedDict()
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(xml_elem)
            json_elem_list.append(json_elem)
        return json_elem_list


class Salary:
    """Extracts and transforms the Salary course element"""

    def __init__(self):
        self.xml_element_key = "SALARY"
        self.xml_subj_key = "SALSBJ"
        self.xml_agg_key = "SALAGG"
        self.xml_unavail_reason_key = "SALUNAVAILREASON"

        self.shared_utils = SharedUtils(
            self.xml_element_key,
            self.xml_subj_key,
            self.xml_agg_key,
            self.xml_unavail_reason_key,
        )

        self.salary_data_fields_lookup = self.shared_utils.get_lookup(
            "salary_data_fields"
        )

    def get_json_data(self, xml_elem):

        # TODO: Use this technique where appropriate elsewhere.
        lookup = self.salary_data_fields_lookup
        json_data = {}
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                json_data[lookup[xml_key][0]] = self.shared_utils.get_json_value(
                    xml_elem[xml_key]
                )
            else:
                if xml_key in xml_elem:
                    json_key = lookup[xml_key][0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(xml_elem)
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_stats(self, raw_course_data):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, self.xml_element_key)
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(xml_elem)
            sorted_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(sorted_json_elem)
        return json_elem_list


class Tariff:
    """Extracts and transforms the Tariff course element"""

    def __init__(self):
        self.xml_element_key = "TARIFF"
        self.xml_subj_key = "TARSBJ"
        self.xml_agg_key = "TARAGG"
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

    def get_tariff_description(self, xml_key):
        return self.tariff_description_lookup[xml_key]

    def get_tariffs_list(self, xml_elem):
        return [
            {
                "code": xml_key,
                "description": self.get_tariff_description(xml_key),
                "entrants": int(xml_elem[xml_key]),
            }
            for xml_key in self.tariff_description_lookup.keys()
        ]

    def get_json_data(self, xml_elem):
        json_data = {}
        json_data["aggregation_level"] = int(xml_elem[self.xml_agg_key])
        json_data["number_of_students"] = int(xml_elem[self.xml_pop_key])
        if self.xml_subj_key in xml_elem:
            json_data["subject"] = self.shared_utils.get_subject(xml_elem)
        json_data["tariffs"] = self.get_tariffs_list(xml_elem)
        return json_data

    def get_stats(self, raw_course_data):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, self.xml_element_key)
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(xml_elem)
            sorted_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(sorted_json_elem)
        return json_elem_list


class SharedUtils:
    """Functionality required by several stats related classes"""

    def __init__(
        self, xml_element_key, xml_subj_key, xml_agg_key, xml_unavail_reason_key
    ):

        self.xml_element_key = xml_element_key
        self.xml_subj_key = xml_subj_key
        self.xml_agg_key = xml_agg_key
        self.xml_unavail_reason_key = xml_unavail_reason_key
        self.subj_code_english = self.get_lookup("subj_code_english")
        self.subj_code_welsh = self.get_lookup("subj_code_welsh")
        self.unavail_reason = self.get_lookup("unavail_reason")

    @staticmethod
    def get_lookup(lookup_name):
        cwd = os.path.dirname(os.path.abspath(__file__))
        filename = {
            "subj_code_english": "subj_code_english.json",
            "subj_code_welsh": "subj_code_welsh.json",
            "unavail_reason": "unavailreason.json",
            "tariff_description": "tariff_description.json",
            "salary_data_fields": "salary_data_fields.json",
            "nss_question_description": "nss_question_description.json",
            "nss_data_fields": "nss_data_fields.json",
            "nhs_question_description": "nhs_question_description.json",
            "nhs_data_fields": "nhs_data_fields.json",
            "leo_unavail_reason": "leo_unavailreason.json",
            "leo_data_fields": "leo_data_fields.json",
            "common_data_fields": "common_data_fields.json",
        }[lookup_name]
        with open(os.path.join(cwd, f"lookup_files/{filename}")) as infile:
            return json.load(infile)

    def get_english_sbj_label(self, code):
        return self.subj_code_english[code]

    def get_welsh_sbj_label(self, code):
        return self.subj_code_welsh[code]

    def get_subject(self, xml_elem):
        subj_key = xml_elem[self.xml_subj_key]
        subject = {}
        subject["code"] = subj_key
        subject["english_label"] = self.get_english_sbj_label(subj_key)
        subject["welsh_label"] = self.get_welsh_sbj_label(subj_key)
        return subject

    def get_aggs_for_code(self, unavail_reason_code):
        return self.unavail_reason["data"][unavail_reason_code].keys()

    def need_unavailable(self, xml_elem):
        """Returns True if unavailable is needed otherwise False"""
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
        return "this subject"

    def get_unavailable_reason_str(self, unavail_reason_code, subj_key, agg, xml_elem):
        validate_unavailable_reason_code(unavail_reason_code)

        if not self.has_data(xml_elem):
            reason_str = self.unavail_reason["no-data"][unavail_reason_code]
            return unicodedata.normalize("NFKD", reason_str)

        validate_agg(unavail_reason_code, agg, self.unavail_reason)
        partial_reason_str = self.unavail_reason["data"][unavail_reason_code][agg]
        partial_reason_str = unicodedata.normalize("NFKD", partial_reason_str)
        subj = self.get_unavailable_reason_subj(subj_key)

        # Handle unavailable reason for aggregation over 2 years
        if agg == "21" or agg == "22" or agg == "23":
            return partial_reason_str + subj + " across the last two years."
        elif agg == "24":
            return partial_reason_str

        return partial_reason_str + subj + "."

    def get_unavailable(self, elem):
        unavailable = {}
        subj_key = elem.get(self.xml_subj_key)
        agg = elem[self.xml_agg_key] if self.has_data(elem) else None
        unavail_reason_code = elem[self.xml_unavail_reason_key]
        validate_unavailable_reason_code(unavail_reason_code)

        unavailable["code"] = int(unavail_reason_code)
        unavailable["reason"] = self.get_unavailable_reason_str(
            unavail_reason_code, subj_key, agg, elem
        )
        return unavailable

    def get_json_list(self, raw_course_data, get_key):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(raw_course_data, self.xml_element_key)
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
                    json_elem[json_key] = self.get_json_value(xml_elem[xml_key])
                ordered_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(ordered_json_elem)
        return json_elem_list

    @staticmethod
    def has_data(xml_elem):
        """Returns True if the stats XML element has data otherwise False"""
        return len(xml_elem) > 1

    @staticmethod
    def get_json_value(xml_value):
        if xml_value.isdigit():
            xml_value = int(xml_value)
        return xml_value

    @staticmethod
    def get_raw_list(data, element_key):
        """If value is a dict, return in list, otherwise return value"""
        if isinstance(data[element_key], dict):
            return [data[element_key]]
        return data[element_key]
