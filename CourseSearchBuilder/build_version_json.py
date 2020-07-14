"""Module for creating the instituions.json file used by CMS"""

import json
import io
import os

from SharedCode.dataset_helper import DataSetHelper
from SharedCode.blob_helper import BlobHelper


def build_version_json_file():
    # TODO: Ensure that UseLocalTestXMLFile is set to false in local.settings.json before going live.
    use_local_test_XML_file = os.environ.get('UseLocalTestXMLFile')
    use_local_test_version = os.environ.get('UseLocalTestVersion')

    if use_local_test_XML_file:
        version = use_local_test_version
    else:
        version = DataSetHelper().get_latest_version_number()

    blob_helper = BlobHelper()

    version_file = io.StringIO()

    version_json = {}
    version_json["version"] = version


    json.dump(version_json, version_file, indent=4)
    encoded_file = version_file.getvalue().encode('utf-8')

    storage_container_name = os.environ["AzureStorageJSONFilesContainerName"]
    blob_helper.write_stream_file(storage_container_name, "version.json", encoded_file)
