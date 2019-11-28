import os
import #logging
import traceback

import azure.functions as func
from SharedCode.dataset_helper import DataSetHelper
from SharedCode import utils
from . import search


def main(dataset: func.InputStream):

    try:

        dsh = DataSetHelper()

        #logging.info(
            f"Python blob trigger function processed blob \n"
            f"Name: {dataset.name}\n"
            f"Blob Size: {dataset.length} bytes\n"
        )

        api_key = os.environ["SearchAPIKey"]
        search_url = os.environ["SearchURL"]
        api_version = os.environ["AzureSearchAPIVersion"]

        version = dsh.get_latest_version_number()

        dsh.update_status("search", "in progress")

        search.build_synonyms(search_url, api_key, api_version)

        search.build_index(search_url, api_key, api_version, version)

        courses = utils.get_courses_by_version(version)

        number_of_courses = len(courses)

        #logging.info(
            f"attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n"
        )

        search.load_index(search_url, api_key, api_version, version, courses)
        dsh.update_status("search", "succeeded")

        if dsh.have_all_builds_succeeded():
            dsh.update_status("root", "succeeded")
        else:
            dsh.update_status("root", "failed")

        #logging.info(f"Successfully loaded search documents\n")

    except Exception as e:
        # Unexpected exception
        dsh.update_status("search", "failed")
        dsh.update_status("root", "failed")

        #logging.error("Unexpected exception")
        #logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
