"""Data extraction and transformation for statistical data."""
import unicodedata
from typing import Any
from typing import Dict
from typing import Tuple

from .stats.continuation import Continuation
from .stats.employment import Employment
from .stats.entry import Entry
from .stats.job_list import JobList
from .stats.job_type import JobType
from .stats.nhs_nss import NhsNss
from .stats.nss import Nss
from .stats.shared_utils import SharedUtils
from .stats.tariff import Tariff


def get_stats(raw_course_data: Dict[str, Any], subject_codes: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes a course data dictionary and creates a stats data dictionary such as employment
    after the course, NSS results, etc.

    :param raw_course_data: Dictionary containing course data
    :type raw_course_data: Dict[str, Any]
    :param subject_codes: Subject codes to extract stats from
    :type subject_codes: Dict[str, Any]
    :return: Dictionary containing statistics data for a course
    """
    continuation = Continuation(subject_codes)
    employment = Employment(subject_codes)
    entry = Entry(subject_codes)
    job_type = JobType(subject_codes)
    job_list = JobList(subject_codes)
    nhs_nss = NhsNss(subject_codes)
    nss = Nss(subject_codes)
    tariff = Tariff(subject_codes)

    stats = {
        "continuation": continuation.get_stats(raw_course_data),
        "employment": employment.get_stats(raw_course_data),
        "entry": entry.get_stats(raw_course_data),
        "job_type": job_type.get_stats(raw_course_data),
        "job_list": job_list.get_stats(raw_course_data)
    }
    if need_nhs_nss(raw_course_data):
        stats["nhs_nss"] = nhs_nss.get_stats(raw_course_data)
    stats["nss"] = nss.get_stats(raw_course_data)
    stats["tariff"] = tariff.get_stats(raw_course_data)
    return stats


def need_nhs_nss(course: Dict[str, Any]) -> bool:
    """
    Takes a course data dictionary and checks if the NHS NSS results are required.
    Returns True if they are, else False

    :param course: Dictionary containing course data
    :type course: Dict[str, Any]
    :return: True if the NHS NSS results are required, else False
    :rtype: bool
    """
    nhs_nss_elem = SharedUtils.get_raw_list(course, "NHSNSS")[0]
    if SharedUtils.has_data(nhs_nss_elem):
        return True
    return False


# apw for unavail messages
def get_earnings_unavail_text(
        subject_codes: dict[str, dict[str, str]],
        inst_or_sect: str,
        data_source: str,
        key_level_3: str
) -> Tuple[str, str]:
    """
    Returns the relevant unavail reason text in English and Welsh

    :param subject_codes: Subject codes for shared utils object
    :type subject_codes: dict[str, dict[str, str]]
    :param inst_or_sect: Key for institution or sector for extracting unavailable text
    :type inst_or_sect: str
    :param data_source: Key for data source for extracting unavailable text
    :type data_source: str
    :param key_level_3: Third level key for extracting unavailable text
    :type key_level_3: str
    :return: Tuple containing the English and Welsh unavailable text strings
    :rtype: Tuple[str, str]
    """

    shared_utils = SharedUtils(
        subject_codes=subject_codes,
        xml_element_key=data_source,  # xml_element_key
        xml_subj_key="SBJ",
        xml_agg_key="AGG",
        xml_unavail_reason_key='UNAVAILREASON',  # xml_unavail_reason_key
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
