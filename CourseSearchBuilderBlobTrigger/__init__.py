import os
import traceback

import azure.functions as func
from SharedCode.dataset_helper import DataSetHelper
from SharedCode import utils
from . import search


def main(dataset: func.InputStream):

    try:

        dsh = DataSetHelper()


        api_key = os.environ["SearchAPIKey"]
        search_url = os.environ["SearchURL"]
        api_version = os.environ["AzureSearchAPIVersion"]

        version = dsh.get_latest_version_number()

        dsh.update_status("search", "in progress")

        search.build_synonyms(search_url, api_key, api_version)

        search.build_index(search_url, api_key, api_version, version)

        courses = utils.get_courses_by_version(version)

        number_of_courses = len(courses)

        search.load_index(search_url, api_key, api_version, version, courses)
        dsh.update_status("search", "succeeded")

        if dsh.have_all_builds_succeeded():
            dsh.update_status("root", "succeeded")
        else:
            dsh.update_status("root", "failed")

    except Exception as e:
        # Unexpected exception
        dsh.update_status("search", "failed")
        dsh.update_status("root", "failed")

        # Raise to Azure
        raise e
