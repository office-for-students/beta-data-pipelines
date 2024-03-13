import logging
import traceback
from datetime import datetime

from decouple import config

from legacy.services import utils
from legacy.services.dataset_service import DataSetService
# from SharedCode.mail_helper import MailHelper
from . import search
from .build_institutions_json import build_institutions_json_files
from .build_sitemap_xml import build_sitemap_xml
from .build_subjects_json import build_subjects_json_file
from .build_version_json import build_version_json_file


def build_search() -> None:
    dsh = DataSetService()

    try:

        logging.info(
            f"CourseSearchBuilder message queue triggered \n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CourseSearchBuilder function started on {function_start_datetime}"
        )

        api_key = config("SEARCH_API_KEY")
        search_url = config("SEARCH_URL")
        api_version = config("SEARCH_API_VERSION")

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
        courses = None

        if dsh.have_all_builds_succeeded():
            build_institutions_json_files()
            build_subjects_json_file()
            build_version_json_file()
            build_sitemap_xml()

            dsh.update_status("root", "succeeded")
        else:
            dsh.update_status("root", "failed")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_end_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import completed on {function_end_datetime}" + msgin.get_body().decode("utf-8") + msgerror,
        #     f"Data Import {environment} - {function_end_date} - Completed"
        # )

        logging.info(
            f"CourseSearchBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        dsh.update_status("search", "failed")
        dsh.update_status("root", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        # mail_helper.send_message(
        #     f"Automated data import failed on {function_fail_datetime} at CourseSearchBuilder" + msgin.get_body().decode("utf-8") + msgerror,
        #     f"Data Import {environment} - {function_fail_date} - Failed"
        # )

        logging.error(f"CourseSearchBuilder failed on {function_fail_datetime}")
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
