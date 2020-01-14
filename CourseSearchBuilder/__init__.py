import os
import logging
import traceback

from datetime import datetime

import azure.functions as func
from SharedCode.dataset_helper import DataSetHelper
from SharedCode.mail_helper import MailHelper
from SharedCode import utils
from . import search


def main(msgin: func.QueueMessage):

    try:

        dsh = DataSetHelper()

        logging.info(
            f"CourseSearchBuilder message queue triggered \n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        mail_helper = MailHelper()
        environment = os.environ["Environment"]

        logging.info(
            f"CourseSearchBuilder function started on {function_start_datetime}"
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

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_end_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Automated data import completed on {function_end_datetime}", f"Data Import {environment} - {function_end_date} - Completed")

        logging.info(
            f"CourseSearchBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        dsh.update_status("search", "failed")
        dsh.update_status("root", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        mail_helper.send_message(f"Automated data import failed on {function_fail_datetime} at CourseSearchBuilder", f"Data Import {environment} - {function_fail_date} - Failed")

        logging.error(f"CourseSearchBuilder failed on {function_fail_datetime}")
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
