from datetime import datetime

from CourseSearchBuilder.get_collections import get_institutions, get_collections
from SharedCode.blob_helper import BlobHelper

base_url = "https://discoveruni.gov.uk"


def build_sitemap_xml() -> None:
    blob_helper = BlobHelper()
    institution_list = get_institutions()
    course_list = get_collections("AzureCosmosDbCoursesCollectionId")
    institution_params, course_params = build_param_lists(institution_list, course_list)
    xml = """
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
    xml_data = build_xml_string(institution_params + course_params, xml)


def build_param_lists(institution_list: list, course_list: list) -> tuple:
    institution_params = get_institution_params(institution_list)
    course_params = get_course_params(course_list)
    return institution_params, course_params


def get_institution_params(institution_list: list) -> list:
    institution_params = list()
    for institution in institution_list:
        institution_id = institution.get("institution", {}).get("pub_ukprn")
        institution_params += [(institution_id, "en"), (institution_id, "cy")]
    return institution_params


def get_course_params(course_list: list) -> list:
    course_params = list()
    for course in course_list:
        institution_id = course.get("institution_id")
        course_id = course.get("course", {}).get("kis_course_id")
        kis_mode = course.get("course", {}).get("mode", {}).get("label")
        course_params += [(institution_id, course_id, kis_mode, "en"), (institution_id, course_id, kis_mode, "cy")]
    return course_params


def build_xml_string(arg_list: tuple, xml: str) -> str:
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
    return f"{base_url}/{language}/course-details/{institution_id}/{course_id}/{kis_mode}/"


def build_institution_details_url(institution_id: str, language: str) -> str:
    return f"{base_url}/{language}/institution-details/{institution_id}/"
