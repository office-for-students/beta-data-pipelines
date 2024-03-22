"""Module for creating the instituions.json file used by CMS"""

import io
import json

from constants import BLOB_JSON_FILES_CONTAINER_NAME
from constants import BLOB_VERSION_JSON_FILE_BLOB_NAME
from legacy.services.blob import BlobService
from legacy.services.dataset_service import DataSetService


def build_version_json_file(
        blob_service: BlobService,
        dataset_service: DataSetService
) -> None:
    """
    Calls required functions to generate a version.json file which contains the version of the dataset service

    :param blob_service: Blob service used to store the JSON file
    :type blob_service: BlobService
    :param dataset_service: Dataset service used to store the JSON file
    :type dataset_service: DataSetService
    """
    version = dataset_service.get_latest_version_number()

    version_file = io.StringIO()

    version_json = {"version": version}

    json.dump(version_json, version_file, indent=4)
    encoded_file = version_file.getvalue().encode('utf-8')

    blob_service.write_stream_file(BLOB_JSON_FILES_CONTAINER_NAME, BLOB_VERSION_JSON_FILE_BLOB_NAME, encoded_file)
