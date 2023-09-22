"""Data extraction and transformation for statistical data."""

import json
import logging
import os
from collections import OrderedDict
from typing import Dict
from typing import List
from typing import Tuple

import unicodedata

from EtlPipeline.validators import validate_agg
from EtlPipeline.validators import validate_unavailable_reason_code
from SharedCode.dataset_helper import DataSetHelper
from SharedCode.utils import get_subject_lookups


def get_stats(raw_course_data, country_code):
    continuation = Continuation()
    employment = Employment()
    entry = Entry()
    job_type = JobType()
    job_list = JobList()
    nhs_nss = NhsNss()
    nss = Nss()
    tariff = Tariff()

    stats = {}
    stats["continuation"] = continuation.get_stats(raw_course_data)
    stats["employment"] = employment.get_stats(raw_course_data)
    stats["entry"] = entry.get_stats(raw_course_data)
    stats["job_type"] = job_type.get_stats(raw_course_data)
    stats["job_list"] = job_list.get_stats(raw_course_data)
    if need_nhs_nss(raw_course_data):
        stats["nhs_nss"] = nhs_nss.get_stats(raw_course_data)
    stats["nss"] = nss.get_stats(raw_course_data)
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
            "CONTAGGYEAR": "aggregation_year",
            "CONTYEAR1": "aggregation_year_1",
            "CONTYEAR2": "aggregation_year_2",
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
            "EMPRESPONSE": "response_not_used",
            "EMPSAMPLE": "sample_not_used",
            "EMPRESP_RATE": "response_rate",
            "EMPAGG": "aggregation_level",
            "EMPAGGYEAR": "aggregation_year",
            "EMPYEAR1": "aggregation_year_1",
            "EMPYEAR2": "aggregation_year_2",
            "EMPYEAR3": "aggregation_year_3",
            "EMPSBJ": "subject",
            "WORKSTUDY": "in_work_or_study",
            "PREVWORKSTUD": "unemp_prev_emp_since_grad",
            "STUDY": "doing_further_study",
            "UNEMP": "unemp_not_work_since_grad",
            "BOTH": "in_work_and_study",
            "NOAVAIL": "other",
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
            "ENTRESPONSE": "response_not_used",
            "ENTSAMPLE": "sample_not_used",
            "ENTAGG": "aggregation_level",
            "ENTAGGYEAR": "aggregation_year",
            "ENTYEAR1": "aggregation_year_1",
            "ENTYEAR2": "aggregation_year_2",
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
            "JOBRESPONSE": "response_not_used",
            "JOBSAMPLE": "sample_not_used",
            "JOBAGG": "aggregation_level",
            "JOBAGGYEAR": "aggregation_year",
            "JOBYEAR1": "aggregation_year_1",
            "JOBYEAR2": "aggregation_year_2",
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
        self.data_fields_lookup = self.shared_utils.get_lookup(
            "common_data_fields"
        )

    def get_stats(self, raw_course_data):
        """Extracts and transforms the COMMON entries in a KISCOURSE"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(
                    xml_elem
                )
            json_elem_list.append(json_elem)
        return json_elem_list

    def get_json_data(self, xml_elem):
        """Extracts and transforms a COMMON entry with data in a KISCOURSE"""

        lookup = self.data_fields_lookup
        json_data = {}
        for xml_key in lookup:
            if lookup[xml_key][1] == "M":
                json_data[
                    lookup[xml_key][0]
                ] = self.shared_utils.get_json_value(xml_elem[xml_key])
            else:
                if xml_key in xml_elem:
                    json_key = lookup[xml_key][0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(
                            xml_elem
                        )
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

            if job_list_item["percentage_of_students"].isnumeric():
                if int(job_list_item["percentage_of_students"]) < 5:
                    job_list_item["percentage_of_students"] = "<5"

            job_list_item["order"] = job_list["ORDER"]
            job_list_item["hs"] = job_list["HS"]
            list_field.append(job_list_item)
        return list_field


class Nss:
    """Extracts and transforms the NSS course element"""

    NUM_QUESTIONS = 28

    def __init__(self):
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
                    json_key = lookup[xml_key][0]
                    if json_key == "subject":
                        json_data[json_key] = self.shared_utils.get_subject(
                            xml_elem
                        )
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_stats(self, raw_course_data):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        secondary_xml_list = SharedUtils.get_raw_list(
            raw_course_data, "NSSCOUNTRY"
        )
        json_elem = dict()
        for xml_elem in raw_xml_list:
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(
                    xml_elem
                )
            json_elem_list.append(json_elem)

        for xml_elem in secondary_xml_list:
            self.shared_utils_nss_country = SharedUtils(
                "NSSCOUNTRY", "NSSSBJ", "NSSAGG", "NSSCOUNTRYUNAVAILREASON"
            )
            if self.shared_utils_nss_country.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils_nss_country.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils_nss_country.get_unavailable(
                    xml_elem
                )
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
        self.data_fields_lookup = self.shared_utils.get_lookup(
            "nhs_data_fields"
        )
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
                        json_data[json_key] = self.shared_utils.get_subject(
                            xml_elem
                        )
                    else:
                        json_data[json_key] = self.shared_utils.get_json_value(
                            xml_elem[xml_key]
                        )
        return json_data

    def get_stats(self, raw_course_data):
        """Returns a list of JSON objects (as dicts) for this stats element"""

        json_elem_list = []
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        for xml_elem in raw_xml_list:
            json_elem = OrderedDict()
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(
                    xml_elem
                )
            json_elem_list.append(json_elem)
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
        raw_xml_list = SharedUtils.get_raw_list(
            raw_course_data, self.xml_element_key
        )
        for xml_elem in raw_xml_list:
            json_elem = {}
            if self.shared_utils.has_data(xml_elem):
                json_elem.update(self.get_json_data(xml_elem))
            if self.shared_utils.need_unavailable(xml_elem):
                json_elem["unavailable"] = self.shared_utils.get_unavailable(
                    xml_elem
                )
            sorted_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(sorted_json_elem)
        return json_elem_list


class SharedUtils:
    """Functionality required by several stats related classes"""

    try:
        dsh = DataSetHelper()  # TODO: warning presented as dsh wasn't assigned, but the catch all
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
    ):

        self.xml_element_key = xml_element_key
        self.xml_subj_key = xml_subj_key
        self.xml_agg_key = xml_agg_key
        self.xml_unavail_reason_key = xml_unavail_reason_key
        self.subj_code_english = self.get_lookup("subj_code_english")
        self.subj_code_welsh = self.get_lookup("subj_code_welsh")
        self.unavail_reason_english = self.get_lookup("unavail_reason_english")
        self.unavail_reason_welsh = self.get_lookup("unavail_reason_welsh")

    @staticmethod
    def get_lookup(lookup_name):
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
        }[lookup_name]
        with open(os.path.join(cwd, f"lookup_files/{filename}")) as infile:
            return json.load(infile)

    def get_subject(self, xml_elem):
        subj_key = xml_elem[self.xml_subj_key]
        subject = {}
        subject["code"] = subj_key
        subject["english_label"] = self.get_english_sbj_label(subj_key)
        subject["welsh_label"] = self.get_welsh_sbj_label(subj_key)
        return subject

    def get_english_sbj_label(self, code):
        if SharedUtils.subj_codes != {}:
            return SharedUtils.subj_codes[code].get("english_name")
        return self.subj_code_english[code]

    def get_welsh_sbj_label(self, code):
        if SharedUtils.subj_codes != {}:
            return SharedUtils.subj_codes[code].get("welsh_name")
        return self.subj_code_welsh[code]

    def get_json_list(self, raw_course_data, get_key):
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
                        xml_elem[xml_key]
                    )
                ordered_json_elem = OrderedDict(sorted(json_elem.items()))
            json_elem_list.append(ordered_json_elem)
        return json_elem_list

    def need_unavailable(self, xml_elem):
        if not self.has_data(xml_elem):
            return True

        unavail_reason_code = xml_elem[self.xml_unavail_reason_key]
        try:
            agg = xml_elem[self.xml_agg_key]
            agg_codes = self.get_aggs_for_code(unavail_reason_code)
            if agg in agg_codes:
                return True
        except KeyError:
            logging.warning("course does not have agg code")
        return False

    def get_aggs_for_code(self, unavail_reason_code):
        return self.unavail_reason_english["data"][unavail_reason_code].keys()

    def get_unavailable(self, elem):
        unavailable = {}
        subj_key = elem.get(self.xml_subj_key)
        agg = elem[self.xml_agg_key] if self.has_data(elem) else None
        if agg == "14":
            return ""

        unavail_reason_code = elem[self.xml_unavail_reason_key]
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
            self, unavail_reason_code, subj_key, agg, xml_elem, resp_rate_state, welsh=False
    ):
        validate_unavailable_reason_code(unavail_reason_code)
        if welsh:
            unavail_reason_lookup = self.unavail_reason_welsh
        else:
            unavail_reason_lookup = self.unavail_reason_english

        if not self.has_data(xml_elem):
            reason_str = unavail_reason_lookup["no-data"][unavail_reason_code]
            return unicodedata.normalize("NFKD", reason_str)

        validate_agg(unavail_reason_code, agg, unavail_reason_lookup)

        # if unavail_reason_code == 0 and agg and agg != "14" and agg != "" and agg is not None:
        #     partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code][agg][resp_rate_state]
        # elif unavail_reason_code != 0:
        #     partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code][agg]
        # The lookup tables do not contain entries for code=0, agg=14 or code=0, agg=None.
        #    No unavail message needs to be displayed in either of these scenarios.
        if unavail_reason_code == '0':
            partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code][agg][resp_rate_state]
        else:
            partial_reason_str = unavail_reason_lookup["data"][unavail_reason_code][agg]

        partial_reason_str = unicodedata.normalize("NFKD", partial_reason_str)
        if welsh:
            subj = self.get_unavailable_reason_subj_welsh(subj_key)
        else:
            subj = self.get_unavailable_reason_subj_english(subj_key)

        return partial_reason_str.replace("[Subject]", subj)

    def get_unavailable_reason_subj_english(self, sbj_key):
        if sbj_key:
            return self.get_english_sbj_label(sbj_key)
        return self.unavail_reason_english["no-subject"]

    def get_unavailable_reason_subj_welsh(self, sbj_key):
        if sbj_key:
            return self.get_welsh_sbj_label(sbj_key)
        return self.unavail_reason_welsh["no-subject"]

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

    @staticmethod
    def get_raw_lists(data, element_key: List) -> List:
        for key in element_key:
            if isinstance(data[key], dict):
                return [data[key]]
            return data[key]


def need_nhs_nss(course):
    nhs_nss_elem = SharedUtils.get_raw_list(course, "NHSNSS")[0]
    if SharedUtils.has_data(nhs_nss_elem):
        return True
    return False


# apw for unavail messages
def get_earnings_unavail_text(inst_or_sect, data_source, key_level_3) -> Tuple[str, str]:
    """Returns the relevant unavail reason text in English and Welsh"""

    shared_utils = SharedUtils(
        data_source,  # xml_element_key
        "SBJ",
        "AGG",
        'UNAVAILREASON',  # xml_unavail_reason_key
    )

    earnings_unavail_reason_lookup_english = shared_utils.get_lookup(
        "earnings_unavail_reason_english"
    )
    earnings_unavail_reason_lookup_welsh = shared_utils.get_lookup(
        "earnings_unavail_reason_welsh"
    )

    unavail_text_english = earnings_unavail_reason_lookup_english[inst_or_sect][data_source][key_level_3]
    unavail_text_welsh = earnings_unavail_reason_lookup_welsh[inst_or_sect][data_source][key_level_3]

    return unicodedata.normalize("NFKD", unavail_text_english), \
           unicodedata.normalize("NFKD", unavail_text_welsh)
