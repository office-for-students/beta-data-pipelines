import logging
from typing import Any
from typing import Dict
from typing import TYPE_CHECKING
from typing import Union

from constants import COSMOS_COLLECTION_INSTITUTIONS
from constants import COSMOS_COLLECTION_SUBJECTS
from legacy.EtlPipeline.stats.shared_utils import SharedUtils

if TYPE_CHECKING:
    from legacy.EtlPipeline.subject_enricher import SubjectCourseEnricher


# TODO: **House-keeping** g_subject_enricher review why this is setup this way

def get_subject(subject_code: str, subject_enricher: type['SubjectCourseEnricher']) -> Dict[str, Any]:
    """
    Takes a subject code and SubjectCourseEnricher, and returns the enriched subject data.

    :param subject_code: Subject code lookup
    :type subject_code: str
    :param subject_enricher: Subject enricher object
    :type subject_enricher: SubjectCourseEnricher
    :return: Enriched subject data dictionary
    :rtype: Dict[str, Any]
    """
    try:
        english_label = subject_enricher.subject_lookups[subject_code]["english_name"]
    except KeyError:
        logging.warning(f"English label for subject code {subject_code} not found")
        english_label = "Unknown"
    try:
        welsh_label = subject_enricher.subject_lookups[subject_code]["welsh_name"]
    except KeyError:
        logging.warning(f"Welsh label for subject code {subject_code} not found")
        welsh_label = "Unknown"
    subject = {
        "code": subject_code,
        "english_label": english_label,
        "welsh_label": welsh_label
    }

    return subject


def get_go_work_unavail_messages(
        xml_element_key: str,
        xml_agg_key: str,
        xml_unavail_reason_key: str,
        raw_data_element: Dict[str, Any],
        subject_codes: dict[str, dict[str, str]]
) -> Union[Dict[str, Any], str]:
    """
    Constructs a SharedUtils object and creates unavailable message data based on the passed parameters.

    :param xml_element_key: XML element key for unavailable message
    :type xml_element_key: str
    :param xml_agg_key: Aggregation code for unavailable message
    :type xml_agg_key: str
    :param xml_unavail_reason_key: Key for unavailable reason
    :type xml_unavail_reason_key: str
    :param raw_data_element: Raw data element to generate unavailable message for
    :type raw_data_element: str
    :param subject_codes: Subject codes for shared utils object
    :type subject_codes: dict[str, dict[str, str]]
    :return: Dictionary of unavailable message data, or empty string if aggregation code is "14"
    :rtype: Union[Dict[str, Any], str]
    """
    shared_utils = SharedUtils(
        xml_element_key=xml_element_key,
        xml_subj_key="GOWORKSBJ",
        xml_agg_key=xml_agg_key,
        xml_unavail_reason_key=xml_unavail_reason_key,
        subject_codes=subject_codes
    )
    return shared_utils.get_unavailable(raw_data_element)


def get_earnings_agg_unavail_messages(agg_value: str, subject: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes an aggregation code and a subject data dictionary, and returns the earnings unavailable message for
    these parameters.
    Returns an empty dictionary if the aggregation code is not "21" or "22".

    :param agg_value: Aggregation code
    :type agg_value: str
    :param subject: Subject data
    :type subject: Dict[str, Any]
    :return: Unavailable message data
    :rtype: Dict[str, Any]
    """
    earnings_agg_unavail_messages = {}

    if agg_value in ['21', '22']:
        message_english = "The data displayed is from students on this and other courses in [Subject].\n\nThis " \
                          "includes data from this and related courses at the same university or college. There " \
                          "was not enough data to publish more specific information. This does not reflect on the " \
                          "quality of the course."
        message_welsh = "Daw'r data a ddangosir gan fyfyrwyr ar y cwrs hwn a chyrsiau [Subject] eraill.\n\nMae " \
                        "hwn yn cynnwys data o'r cwrs hwn a chyrsiau cysylltiedig yn yr un brifysgol neu goleg. " \
                        "Nid oedd digon o ddata ar gael i gyhoeddi gwybodaeth fwy manwl. Nid yw hyn yn adlewyrchu " \
                        "ansawdd y cwrs."

        earnings_agg_unavail_messages['english'] = message_english.replace("[Subject]", subject['english_label'])
        earnings_agg_unavail_messages['welsh'] = message_welsh.replace("[Subject]", subject['welsh_label'])

    return earnings_agg_unavail_messages


def get_ukrlp_lookups(cosmos_service: type['CosmosService'], version: int) -> Dict[str, Any]:
    """
    Returns a dictionary of UKRLP lookups, including English and Welsh institution names.

    :param cosmos_service: Cosmos service with database to query the UKRLP lookups from
    :type cosmos_service: CosmosService
    :param version: Dataset version to perform UKRLP lookups on
    :type version: int
    :return: UKRLP lookups as a dictionary
    :rtype: Dict[str, Any]
    """

    container = cosmos_service.get_container(container_id=COSMOS_COLLECTION_INSTITUTIONS)
    query = f"SELECT * from c WHERE c.version = {version}"

    lookup_list = list(container.query_items(query, enable_cross_partition_query=True))

    # Previous data from the UKRLP smashed the ukprn number with the pub_ukprn,
    # to limit changes doing the same now.
    for lookup in lookup_list:
        lookup["institution"]["ukprn"] = lookup["institution"]["pub_ukprn"]

    return {
        lookup["institution"]["ukprn"]: {
            "ukprn_name": lookup["institution"]["pub_ukprn_name"],
            "ukprn_welsh_name": lookup["institution"]["pub_ukprn_welsh_name"]
        }
        for lookup in lookup_list
    }


def get_subject_lookups(cosmos_service: type["CosmosService"], version: int) -> Dict[str, Any]:
    """
    Returns a dictionary of subject lookups, including the subject code.

    :param cosmos_service: Cosmos service to get container from
    :param version: Version of dataset to perform lookup on
    :type version: str
    :return: Dictionary of subject lookups containing subject and code
    :rtype: Dict[str, Any]
    """

    container = cosmos_service.get_container(container_id=COSMOS_COLLECTION_SUBJECTS)
    query = f"SELECT * from c WHERE c.version = {version}"

    lookup_list = list(container.query_items(query, enable_cross_partition_query=True))

    return {
        lookup["code"]: lookup
        for lookup in lookup_list
    }
