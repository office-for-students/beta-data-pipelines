"""Module for creating the instituions.json file used by CMS"""

import io
import json

from decouple import config

from legacy.services.blob import BlobService
from legacy.services.dataset_service import DataSetService


def build_version_json_file() -> None:
    version = DataSetService().get_latest_version_number()

    blob_service = BlobService()

    version_file = io.StringIO()

    version_json = {"version": version}

    json.dump(version_json, version_file, indent=4)
    encoded_file = version_file.getvalue().encode('utf-8')

    storage_container_name = config("BLOB_JSON_FILES_CONTAINER_NAME")
    storage_blob_name = config("BLOB_VERSION_JSON_FILE_BLOB_NAME")
    blob_service.write_stream_file(storage_container_name, storage_blob_name, encoded_file)
