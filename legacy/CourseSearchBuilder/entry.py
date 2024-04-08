import logging
import traceback
from datetime import datetime
from typing import Any
from typing import Dict

from constants import COSMOS_COLLECTION_COURSES
from constants import COSMOS_COLLECTION_INSTITUTIONS
from constants import COSMOS_COLLECTION_SUBJECTS
from constants import SEARCH_API_KEY
from constants import SEARCH_API_VERSION
from constants import SEARCH_SERVICE_MODULE
from constants import SEARCH_URL
from services import search_service
from .build_institutions_json import build_institutions_json_files
from .build_sitemap_xml import build_sitemap_xml
from .build_subjects_json import build_subjects_json_file
from .build_version_json import build_version_json_file
from .get_collections import get_collections_as_list


def course_search_builder_main(
        blob_service: type['BlobServiceBase'],
        cosmos_service: type['CosmosServiceBase'],
        dataset_service: type['DataSetServiceBase']
) -> Dict[str, Any]:
    """
    Builds the course search index

    :param blob_service: Blob service to store JSON files
    :type blob_service: BlobService
    :param cosmos_service: Cosmos service to access database with
    :type cosmos_service: CosmosService
    :param dataset_service: Dataset service used to build course search and version JSON
    :type dataset_service: DataSetService
    :return: None
    """
    response = {}
    try:

        logging.info(
            f"CourseSearchBuilder message queue triggered \n"
        )

        function_start_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        logging.info(
            f"CourseSearchBuilder function started on {function_start_datetime}"
        )
        dataset_service.update_status("search", "in progress")

        version = dataset_service.get_latest_version_number()

        collection_query = f"SELECT * from c where c.version = {version}"
        courses_list = get_collections_as_list(
            cosmos_container=cosmos_service.get_container(container_id=COSMOS_COLLECTION_COURSES),
            query=collection_query
        )

        number_of_courses = len(courses_list)

        logging.info(
            f"attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n"
        )

        search_service_obj = search_service.get_current_provider(
            provider_path=SEARCH_SERVICE_MODULE,
            url=SEARCH_URL,
            api_key=SEARCH_API_KEY,
            api_version=SEARCH_API_VERSION,
            version=version,
            docs=courses_list
        )

        search_service_obj.update_microsoft_search()

        dataset_service.update_status("search", "succeeded")

        if dataset_service.have_all_builds_succeeded():
            institution_list = get_collections_as_list(
                cosmos_container=cosmos_service.get_container(container_id=COSMOS_COLLECTION_INSTITUTIONS),
                query=collection_query
            )
            subjects_list = get_collections_as_list(
                cosmos_container=cosmos_service.get_container(container_id=COSMOS_COLLECTION_SUBJECTS),
                query=collection_query
            )

            build_institutions_json_files(
                institution_list=institution_list,
                blob_service=blob_service
            )
            build_subjects_json_file(
                subjects_list=subjects_list,
                blob_service=blob_service
            )
            build_sitemap_xml(
                institution_list=institution_list,
                course_list=courses_list,
                blob_service=blob_service
            )
            build_version_json_file(
                blob_service=blob_service,
                dataset_service=dataset_service
            )

            dataset_service.update_status("root", "succeeded")
        else:
            dataset_service.update_status("root", "failed")

        function_end_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")

        message = f"CourseSearchBuilder successfully finished on {function_end_datetime}"
        logging.info(message)

        response["message"] = message
        response["statusCode"] = 200
    except Exception as e:
        # Unexpected exception
        dataset_service.update_status("search", "failed")
        dataset_service.update_status("root", "failed")

        function_fail_datetime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
        function_fail_date = datetime.today().strftime("%d.%m.%Y")

        message = f"CourseSearchBuilder failed on {function_fail_datetime}"
        exception = traceback.format_exc()
        logging.error(message)
        logging.error(exception)

        response["message"] = message
        response["exception"] = exception
        response["statusCode"] = 500

    return response
