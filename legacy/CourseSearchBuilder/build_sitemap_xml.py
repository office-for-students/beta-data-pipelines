import io
from datetime import datetime
from io import StringIO
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from constants import BLOB_INSTITUTIONS_SITEMAPS_JSON_FILE_BLOB_NAME
from constants import BLOB_JSON_FILES_CONTAINER_NAME

BASE_URL = "https://discoveruni.gov.uk"


def build_sitemap_xml(
        institution_list: List[Dict[str, Any]],
        course_list: List[Dict[str, Any]],
        blob_service: Type['BlobService']
) -> None:
    """
    Calls all required functions to build the sitemap XML.

    :param institution_list: List of institution data for sitemap XML
    :type institution_list: List[Dict[str, Any]]
    :param course_list: List of course data for sitemap XML
    :type course_list: List[Dict[str, Any]]
    :param blob_service: Blob service used to store the sitemap XML
    :type blob_service: BlobService
    """
    institution_params, course_params = build_param_lists(institution_list, course_list)
    xml = """<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
    xml_data = build_xml_string(institution_params + course_params, xml)
    xml_file: StringIO = io.StringIO(xml_data)
    blob_service.write_stream_file(
        container_name=BLOB_JSON_FILES_CONTAINER_NAME,
        blob_name=BLOB_INSTITUTIONS_SITEMAPS_JSON_FILE_BLOB_NAME,
        encoded_file=xml_file.read().encode('utf-8')
    )


def build_param_lists(
        institution_list: List[Dict[str, Any]],
        course_list: List[Dict[str, Any]]
) -> Tuple[List[Tuple[str]], List[Tuple[str]]]:
    """
    Takes a list of institution data and a list of course data and returns a list of tuples for corresponding
    parameters.

    :param institution_list: List of institution data
    :type institution_list: List[Dict[str, Any]]
    :param course_list: List of course data
    :type course_list: List[Dict[str, Any]]
    :return: List of tuples for parameters for institutions and courses
    :rtype: Tuple[List[Tuple[str]], List[Tuple[str]]]
    """
    institution_params = get_institution_params(institution_list)
    course_params = get_course_params(course_list)
    return institution_params, course_params


def get_institution_params(institution_list: List[Dict[str, Any]]) -> List[Tuple[str]]:
    """
    Takes a list of institution data and returns parameters for each institution as a list of tuples.

    :param institution_list: List of course data
    :type institution_list: List[Dict[str, Any]]
    :return: List of parameters for each institution
    :rtype: List[Tuple[str]]
    """
    institution_params = list()
    for institution in institution_list:
        institution_id = institution.get("institution", {}).get("pub_ukprn")
        institution_params += [(institution_id, "en"), (institution_id, "cy")]
    return institution_params


def get_course_params(course_list: List[Dict[str, Any]]) -> List[Tuple[str]]:
    """
    Takes a list of course data and returns parameters for each course as a list of tuples.

    :param course_list: List of course data
    :type course_list: List[Dict[str, Any]]
    :return: List of parameters for each course
    :rtype: List[Tuple[str]]
    """
    course_params = list()
    for course in course_list:
        institution_id = course.get("institution_id")
        course_id = course.get("course", {}).get("kis_course_id")
        kis_mode = course.get("course", {}).get("mode", {}).get("label")
        course_params += [(institution_id, course_id, kis_mode, "en"), (institution_id, course_id, kis_mode, "cy")]
    return course_params


def build_xml_string(arg_list: List[Tuple[str, str, Optional[str], Optional[str]]], xml: str) -> str:
    """
    Takes a list of arguments and XML as a string and returns a sitemap XML.

    :param arg_list: List of arguments to include in sitemap
    :type arg_list: List[Tuple[str, str, Optional[str], Optional[str]]]
    :param xml: XML string to be included in sitemap
    :type xml: str
    :return: XML sitemap string
    :rtype: str
    """
    today = datetime.strftime(datetime.today(), "%Y-%m-%d")
    centre_xml = """"""
    for args in arg_list:
        if len(args) == 4:
            centre_xml += f"""    <url>
                <loc>{build_course_details_url(*args)}></loc>
                <lastmod>{today}</lastmod>
            </url>
        """
        elif len(args) == 2:
            centre_xml += f"""    <url>
                <loc>{build_institution_details_url(*args)}></loc>
                <lastmod>{today}</lastmod>
            </url>
        """
    final_xml = f"""{xml}
        {centre_xml}</urlset>"""
    return final_xml


def build_course_details_url(institution_id: str, course_id: str, kis_mode: str, language: str) -> str:
    """
    Takes an institution ID, course ID, language, and KIS mode and returns a URL to the course details.
    Uses globally-defined base URL.

    :param institution_id: ID of institution
    :type institution_id: str
    :param course_id: ID of course
    :type course_id: str
    :param kis_mode: KIS mode for course
    :type kis_mode: str
    :param language: Language for which to generate URL
    :type language: str
    :return: Constructed URL
    :rtype: str
    """
    return f"{BASE_URL}/{language}/course-details/{institution_id}/{course_id}/{kis_mode}/"


def build_institution_details_url(institution_id: str, language: str) -> str:
    """
    Takes an institution ID and language and returns a URL to the institution details.
    Uses globally-defined base URL.

    :param institution_id: ID of institution
    :type institution_id: str
    :param language: Language for which to generate URL
    :type language: str
    :return: Constructed URL
    :rtype: str
    """
    return f"{BASE_URL}/{language}/institution-details/{institution_id}/"
