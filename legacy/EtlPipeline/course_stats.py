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


def get_stats(raw_course_data: Dict[str, Any]) -> Dict[str, Any]:
    continuation = Continuation()
    employment = Employment()
    entry = Entry()
    job_type = JobType()
    job_list = JobList()
    nhs_nss = NhsNss()
    nss = Nss()
    tariff = Tariff()

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


def need_nhs_nss(course):
    nhs_nss_elem = SharedUtils.get_raw_list(course, "NHSNSS")[0]
    if SharedUtils.has_data(nhs_nss_elem):
        return True
    return False


# apw for unavail messages
def get_earnings_unavail_text(inst_or_sect: str, data_source: str, key_level_3: str) -> Tuple[str, str]:
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
