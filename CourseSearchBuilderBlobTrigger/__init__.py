import os
import logging
import traceback

from datetime import datetime

import azure.functions as func
from SharedCode.dataset_helper import DataSetHelper
from SharedCode import utils
from . import search


def main(msgin: func.QueueMessage):

    try:

        dsh = DataSetHelper()

        logging.info(
            f"CourseSearchBuilderBlobTrigger message queue triggered \n"
        )
        
        function_start_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CourseSearchBuilderBlobTrigger function started on {function_start_datetime}"
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

        logging.info(
            f"attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n"
        )

        search.load_index(search_url, api_key, api_version, version, courses)
        dsh.update_status("search", "succeeded")

        if dsh.have_all_builds_succeeded():
            dsh.update_status("root", "succeeded")
        else:
            dsh.update_status("root", "failed")

        function_end_datetime = datetime.today().strftime("%Y%m%d %H%M%S")
        logging.info(
            f"CourseSearchBuilderBlobTrigger successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        dsh.update_status("search", "failed")
        dsh.update_status("root", "failed")

        logging.error("Unexpected exception")
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
