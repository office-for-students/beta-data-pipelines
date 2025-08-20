import logging
import os

from SharedCode import exceptions
from SharedCode import utils
from SharedCode.dataset_helper import DataSetHelper
from . import search


def build_search_index(dsh: DataSetHelper) -> bool:
    try:
        api_key = os.environ["SearchAPIKey"]
        search_url = os.environ["SearchURL"]
        api_version = os.environ["AzureSearchAPIVersion"]

        version = dsh.get_latest_version_number()

        dsh.update_status("search", "in progress")

        search.build_synonyms(search_url, api_key, api_version)

        search.build_index(search_url, api_key, api_version, version)
        logging.info(f"CourseSearchBuilder function: get course by version '{version}'")
        courses = utils.get_courses_by_version(version)

        number_of_courses = len(courses)

        logging.info(
            f"attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n"
        )

        search.load_index(search_url, api_key, api_version, version, courses)

        dsh.update_status("search", "succeeded")
    except Exception as e:
        raise exceptions.StopEtlPipelineErrorException(f"build_search_index: error thrown while creating search index for version: {version} {e}")

    return True