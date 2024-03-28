import logging
import traceback
from datetime import datetime
from typing import Type

from constants import COSMOS_COLLECTION_COURSES
from constants import COSMOS_COLLECTION_INSTITUTIONS
from constants import COSMOS_COLLECTION_SUBJECTS
from constants import SEARCH_API_KEY
from constants import SEARCH_API_VERSION
from constants import SEARCH_URL
from .build_institutions_json import build_institutions_json_files
from .build_sitemap_xml import build_sitemap_xml
from .build_subjects_json import build_subjects_json_file
from .build_version_json import build_version_json_file
from .get_collections import get_collections
from typing import Dict, Any

from .search import build_index
from .search import build_synonyms
from .search import load_index


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

        build_synonyms(
            url=SEARCH_URL,
            api_key=SEARCH_API_KEY,
            api_version=SEARCH_API_VERSION
        )
        build_index(
            url=SEARCH_URL,
            api_key=SEARCH_API_KEY,
            api_version=SEARCH_API_VERSION,
            version=version
        )

        courses = get_collections(
            cosmos_service=cosmos_service,
            collection_id=COSMOS_COLLECTION_COURSES,
            dataset_service=dataset_service,
            version=version
        )

        number_of_courses = len(courses)

        logging.info(
            f"attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n"
        )

        load_index(
            url=SEARCH_URL,
            api_key=SEARCH_API_KEY,
            api_version=SEARCH_API_VERSION,
            version=version,
            docs=courses
        )
        dataset_service.update_status("search", "succeeded")

        if dataset_service.have_all_builds_succeeded():
            institution_list = get_collections(
                cosmos_service=cosmos_service,
                dataset_service=dataset_service,
                collection_id=COSMOS_COLLECTION_INSTITUTIONS
            )
            subjects_list = get_collections(
                cosmos_service=cosmos_service,
                dataset_service=dataset_service,
                collection_id=COSMOS_COLLECTION_SUBJECTS
            )
            course_list = get_collections(
                cosmos_service=cosmos_service,
                dataset_service=dataset_service,
                collection_id=COSMOS_COLLECTION_COURSES
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
                course_list=course_list,
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
