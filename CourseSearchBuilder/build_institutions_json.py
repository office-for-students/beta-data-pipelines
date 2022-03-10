"""Module for creating the instituions.json file used by CMS"""

import json
import io
import os
import re
import logging
from CourseSearchBuilder.get_collections import get_institutions
from SharedCode.blob_helper import BlobHelper


def build_institutions_json_files():
    blob_helper = BlobHelper()
    institution_list = get_institutions()
    institutions_file = io.StringIO()

    institutions = []
    for val in institution_list:
        institution = val["institution"]
        if isinstance(institution["pub_ukprn_name"], str):
            inst_entry = get_inst_entry(institution["pub_ukprn_name"])
            institutions.append(inst_entry)

    institutions.sort(key=lambda x: x["order_by_name"])

    json.dump(institutions, institutions_file, indent=4)
    encoded_file = institutions_file.getvalue().encode('utf-8')

    storage_container_name = os.environ["AzureStorageJSONFilesContainerName"]
    storage_blob_name = os.environ["AzureStorageInstitutionsENJSONFileBlobName"]
    blob_helper.write_stream_file(storage_container_name, storage_blob_name, encoded_file)
    institutions_file.close()

    institutions_file = io.StringIO()

    institutions = []
    for val in institution_list:
        institution = val["institution"]
        isnt_name = institution["pub_ukprn_name"]
        inst_welsh_name = institution["pub_ukprn_welsh_name"]

        if isinstance(inst_welsh_name, str):
            inst_entry = get_inst_entry(inst_welsh_name)
            institutions.append(inst_entry)
        elif isinstance(isnt_name, str):
            inst_entry = get_inst_entry(isnt_name)
            institutions.append(inst_entry)

    institutions.sort(key=lambda x: x["order_by_name"])

    json.dump(institutions, institutions_file, indent=4)
    encoded_file = institutions_file.getvalue().encode('utf-8')

    storage_container_name = os.environ["AzureStorageJSONFilesContainerName"]
    storage_blob_name = os.environ["AzureStorageInstitutionsCYJSONFileBlobName"]
    blob_helper.write_stream_file(storage_container_name, storage_blob_name, encoded_file)
    institutions_file.close()


def get_inst_entry(name):
    entry = {}
    order_by_name = get_order_by_name(name)
    alphabet = order_by_name[0]
    entry["alphabet"] = alphabet
    entry["name"] = name
    entry["order_by_name"] = order_by_name
    return entry


def get_order_by_name(name):
    name = name.lower()
    name = remove_phrases_from_start(name)
    return name


def remove_phrases_from_start(name):
    if re.search(r"^the university of ", name):
        name = re.sub(r"^the university of ", "", name)
    if re.search(r"^university of ", name):
        name = re.sub(r"^university of ", "", name)
    if re.search(r"^the ", name):
        name = re.sub(r"^the ", "", name)
    if re.search(r"^prifysgol ", name):
        name = re.sub(r"^prifysgol ", "", name)
    if re.search(r"^coleg ", name):
        name = re.sub(r"^coleg ", "", name)
    if re.search(r"^y coleg ", name):
        name = re.sub(r"^y coleg ", "", name)
    if re.search(r"^y brifysgol ", name):
        name = re.sub(r"^y brifysgol ", "", name)
    name = name.strip()
    name = name.replace(",", "")
    return name
