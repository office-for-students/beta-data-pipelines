"""Module for creating the instituions.json file used by CMS"""

import json
import io
import os
import re

from CourseSearchBuilder.get_collections import get_collections
from SharedCode.blob_helper import BlobHelper


def build_subjects_json_file():
    blob_helper = BlobHelper()
    subjects_list = get_collections("AzureCosmosDbSubjectsCollectionId")
    subjects_file = io.StringIO()

    subjects = []
    for subject in subjects_list:
        subject_entry = get_subject_entry(subject)
        subjects.append(subject_entry)

    subjects.sort(key=lambda x: x["english_name"])

    json.dump(subjects, subjects_file, indent=4)
    encoded_file = subjects_file.getvalue().encode('utf-8')

    storage_container_name = os.environ["AzureStorageJSONFilesContainerName"]
    storage_blob_name = os.environ["AzureStorageSubjectsJSONFileBlobName"]
    blob_helper.write_stream_file(storage_container_name, storage_blob_name, encoded_file)
    subjects_file.close()


def get_subject_entry(subject):
    entry = {}
    entry["code"] = subject["code"]
    entry["english_name"] = subject["english_name"]
    entry["welsh_name"] = subject["welsh_name"]
    entry["level"] = str(subject["level"])
    return entry
