import os
import logging
import traceback

from datetime import datetime

import azure.functions as func

from SharedCode import exceptions
from SharedCode.dataset_helper import DataSetHelper
# from SharedCode.mail_helper import MailHelper
from SharedCode import utils

from .build_institutions_json import build_institutions_json_files
from .build_search import build_search_index
from .build_sitemap_xml import build_sitemap_xml
from .build_subjects_json import build_subjects_json_file
from .build_version_json import build_version_json_file


def main(msgin: func.QueueMessage):
    msgerror = ""
    # mail_helper = MailHelper()
    environment = os.environ["Environment"]

    dsh = DataSetHelper()

    try:

        logging.info(
            f"CourseSearchBuilder message queue triggered \n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CourseSearchBuilder function started on {function_start_datetime}"
        )

        build_complete = build_search_index(dsh=dsh)

        if build_complete and dsh.have_all_builds_succeeded():
            build_institutions_json_files()
            build_subjects_json_file()
            build_version_json_file()
            build_sitemap_xml()

            dsh.update_status("root", "succeeded")
        else:
            dsh.update_status("root", "failed")
            raise exceptions.StopEtlPipelineErrorException(f"Build_complete status {build_complete}, All builds are not marked as succeeded")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_end_date = datetime.today().strftime("%d.%m.%Y")

        logging.info(
            f"CourseSearchBuilder successfully finished on {function_end_datetime}"
        )

    except Exception as e:
        # Unexpected exception
        dsh.update_status("search", "failed")
        dsh.update_status("root", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.error(f"CourseSearchBuilder failed on {function_fail_datetime}")
        logging.error(traceback.format_exc())

        # Raise Exception to Azure
        raise e