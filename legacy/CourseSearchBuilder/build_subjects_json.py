"""Module for creating the instituions.json file used by CMS"""

import io
import json
from typing import Any
from typing import Dict

from decouple import config

from legacy.CourseSearchBuilder.get_collections import get_collections
from legacy.services.blob import BlobService


def build_subjects_json_file() -> None:
    blob_service = BlobService()
    subjects_list = get_collections("AzureCosmosDbSubjectsCollectionId")  # subjects
    subjects_file = io.StringIO()

    subjects = []
    for subject in subjects_list:
        subject_entry = get_subject_entry(subject)
        subjects.append(subject_entry)

    subjects.sort(key=lambda x: x["english_name"])
    json.dump(subjects, subjects_file, indent=4)
    encoded_file = subjects_file.getvalue().encode('utf-8')

    storage_container_name = config("BLOB_JSON_FILES_CONTAINER_NAME")
    storage_blob_name = config("BLOB_SUBJECTS_JSON_FILE_BLOB_NAME")
    blob_service.write_stream_file(storage_container_name, storage_blob_name, encoded_file)
    subjects_file.close()


def get_subject_entry(subject: Dict[str, Any]) -> Dict[str, Any]:
    entry = {
        "code": subject["code"],
        "english_name": subject["english_name"],
        "welsh_name": subject["welsh_name"],
        "level": str(subject["level"])
    }
    return entry