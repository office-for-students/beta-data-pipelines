"""Module for creating the instituions.json file used by CMS"""

import io
import json
import os
import re

from SharedCode.blob_helper import BlobHelper
from SharedCode.dataset_helper import DataSetHelper
from SharedCode.utils import get_collection_link
from SharedCode.utils import get_cosmos_client


def build_institutions_json_files():
    version = DataSetHelper().get_latest_version_number()

    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(
        "AzureCosmosDbDatabaseId", "AzureCosmosDbInstitutionsCollectionId"
    )

    query = f"SELECT * from c where c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    institution_list = list(cosmos_db_client.QueryItems(collection_link, query, options))

    generate_file(
        institution_list=institution_list,
        primary_name="pub_ukprn_name",
        secondary_name="pub_ukprn_welsh_name",
        blob_file="AzureStorageInstitutionsENJSONFileBlobName"
    )

    generate_file(
        institution_list=institution_list,
        primary_name="pub_ukprn_welsh_name",
        secondary_name="pub_ukprn_name",
        blob_file="AzureStorageInstitutionsCYJSONFileBlobName"
    )


def notAlreadyInList(name, existing):
    if name not in existing:
        existing.append(name)
        return True
    return False


def generate_file(institution_list, primary_name, secondary_name, blob_file):
    blob_helper = BlobHelper()
    institutions_file = io.StringIO()
    institutions = []
    for val in institution_list:
        institution = val["institution"]
        primary = institution[primary_name]
        secondary = institution[secondary_name]

        if isinstance(primary, str):
            inst_entry = get_inst_entry(primary)
            institutions.append(inst_entry)
        elif isinstance(secondary, str):
            inst_entry = get_inst_entry(secondary)
            institutions.append(inst_entry)

    institutions.sort(key=lambda x: x["order_by_name"])

    de_duped = []
    final = [name if notAlreadyInList(name=name["name"], existing=de_duped) else None for name in institutions]

    json.dump(final, institutions_file, indent=4)
    encoded_file = institutions_file.getvalue().encode('utf-8')

    storage_container_name = os.environ["AzureStorageJSONFilesContainerName"]
    storage_blob_name = os.environ[blob_file]
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
