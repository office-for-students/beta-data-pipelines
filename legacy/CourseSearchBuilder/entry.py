import logging
import traceback
from datetime import datetime

from constants import SEARCH_API_KEY
from constants import SEARCH_API_VERSION
from constants import SEARCH_URL
from legacy.services import utils
from legacy.services.dataset_service import DataSetService
# from SharedCode.mail_helper import MailHelper
from . import search
from .build_institutions_json import build_institutions_json_files
from .build_sitemap_xml import build_sitemap_xml
from .build_subjects_json import build_subjects_json_file
from .build_version_json import build_version_json_file
from legacy.services.blob import BlobService


def course_search_builder_main(blob_service: BlobService, dataset_service: DataSetService) -> None:
    try:

        logging.info(
            f"CourseSearchBuilder message queue triggered \n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CourseSearchBuilder function started on {function_start_datetime}"
        )

        version = dataset_service.get_latest_version_number()

        dataset_service.update_status("search", "in progress")

        search.build_synonyms(SEARCH_URL, SEARCH_API_KEY, SEARCH_API_VERSION)

        search.build_index(SEARCH_URL, SEARCH_API_KEY, SEARCH_API_VERSION, version)

        courses = utils.get_courses_by_version(version)

        number_of_courses = len(courses)

        logging.info(
            f"attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n"
        )

        search.load_index(SEARCH_URL, SEARCH_API_KEY, SEARCH_API_VERSION, version, courses)
        dataset_service.update_status("search", "succeeded")
        courses = None

        if dataset_service.have_all_builds_succeeded():
            build_institutions_json_files()
            build_subjects_json_file()
            build_version_json_file()
            build_sitemap_xml(blob_service)

            dataset_service.update_status("root", "succeeded")
        else:
            dataset_service.update_status("root", "failed")

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
        dataset_service.update_status("search", "failed")
        dataset_service.update_status("root", "failed")

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
