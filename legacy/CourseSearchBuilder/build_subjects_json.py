"""Module for creating the instituions.json file used by CMS"""

import io
import json
from typing import Any
from typing import Dict
from typing import List
from typing import TYPE_CHECKING

from constants import BLOB_JSON_FILES_CONTAINER_NAME
from constants import BLOB_SUBJECTS_JSON_BLOB_NAME

if TYPE_CHECKING:
    from legacy.services.blob import BlobService


def build_subjects_json_file(subjects_list: List[Dict[str, Any]], blob_service: 'BlobService') -> None:
    """
    Calls required functions to generate a JSON containing subject data.
    File is saved to a blob and not returned by the function.

    :param subjects_list: List of subjects to write to JSON
    :type subjects_list: List[Dict[str, Any]]
    :param blob_service: Blob service used to save the file
    :type blob_service: BlobService
    :return: None
    """
    subjects_file = io.StringIO()

    subjects = []
    for subject in subjects_list:
        subject_entry = get_subject_entry(subject)
        subjects.append(subject_entry)

    subjects.sort(key=lambda x: x["english_name"])
    json.dump(subjects, subjects_file, indent=4)
    encoded_file = subjects_file.getvalue().encode('utf-8')

    blob_service.write_stream_file(BLOB_JSON_FILES_CONTAINER_NAME, BLOB_SUBJECTS_JSON_BLOB_NAME, encoded_file)
    subjects_file.close()


def get_subject_entry(subject: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes a dictionary of subject data and returns a dictionary containing the same keys and subject data

    :param subject: Dictionary containing subject data
    :type subject: Dict[str, Any]
    :return: Dictionary of subject data
    :rtype: Dict[str, Any]
    """
    entry = {
        "code": subject["code"],
        "english_name": subject["english_name"],
        "welsh_name": subject["welsh_name"],
        "level": str(subject["level"])
    }
    return entry
