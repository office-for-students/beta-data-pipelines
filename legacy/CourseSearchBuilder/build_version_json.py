"""Module for creating the instituions.json file used by CMS"""

import io
import json

from constants import BLOB_JSON_FILES_CONTAINER_NAME
from constants import BLOB_VERSION_JSON_FILE_BLOB_NAME
from legacy.services.blob import BlobService
from legacy.services.dataset_service import DataSetService


def build_version_json_file() -> None:
    version = DataSetService().get_latest_version_number()

    blob_service = BlobService()

    version_file = io.StringIO()

    version_json = {"version": version}

    json.dump(version_json, version_file, indent=4)
    encoded_file = version_file.getvalue().encode('utf-8')

    blob_service.write_stream_file(BLOB_JSON_FILES_CONTAINER_NAME, BLOB_VERSION_JSON_FILE_BLOB_NAME, encoded_file)
