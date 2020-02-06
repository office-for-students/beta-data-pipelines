"""Module for creating the instituions.json file used by CMS"""

import json
import os
import re

import azure.cosmos.cosmos_client as cosmos_client


def get_collection_link(db_id, collection_id):
    """Create and return collection link based on values passed in"""

    cosmosdb_database_id = os.environ[db_id]
    cosmosdb_collection_id = os.environ[collection_id]

    # Return a link to the relevant CosmosDB Container/Document Collection
    return "dbs/" + cosmosdb_database_id + "/colls/" + cosmosdb_collection_id


def get_cosmos_client():
    cosmosdb_uri = os.environ["AzureCosmosDbUri"]
    cosmosdb_key = os.environ["AzureCosmosDbKey"]

    master_key = "masterKey"

    return cosmos_client.CosmosClient(
        url_connection=cosmosdb_uri, auth={master_key: cosmosdb_key}
    )


def build_institutions_json_files():
    version = os.environ["Version"]
    institution_version = os.environ["InstitutionVersion"]

    if version == "":
        print("set environment variable: version")
        exit()

    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(
        "AzureCosmosDbDatabaseId", "AzureCosmosDbInstitutionCollectionId"
    )

    query = f"SELECT * from c where c.version = {institution_version}"

    options = {"enableCrossPartitionQuery": True}

    institution_list = list(cosmos_db_client.QueryItems(collection_link, query, options))

    institutions = []
    count = 0
    for val in institution_list:
        institution = val["institution"]
        if isinstance(institution["pub_ukprn_name"], str):
            inst_entry = get_inst_entry(institution["pub_ukprn_name"])
            institutions.append(inst_entry)
            count += 1

    institutions = []
    count = 0
    for val in institution_list:
        institution = val["institution"]
        if isinstance(institution["pub_ukprn_welsh_name"], str):
            inst_entry = get_inst_entry(institution["pub_ukprn_welsh_name"])
            institutions.append(inst_entry)
            count += 1
        elif isinstance(institution["pub_ukprn_name"], str):
            inst_entry = get_inst_entry(institution["pub_ukprn_name"])
            institutions.append(inst_entry)
            count += 1

    with open("institutions.json", "w") as fp:
        json.dump(institutions, fp, indent=4)


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
    if re.search(r"^the university of", name):
        name = re.sub(r"^the university of", "", name)
    if re.search(r"^university of", name):
        name = re.sub(r"^university of", "", name)
    if re.search(r"^prifysgol", name):
        name = re.sub(r"^prifysgol", "", name)
    if re.search(r"^coleg", name):
        name = re.sub(r"^coleg", "", name)
    if re.search(r"^y coleg", name):
        name = re.sub(r"^y coleg", "", name)
    if re.search(r"^y brifysgol", name):
        name = re.sub(r"^y brifysgol", "", name)
    name = name.strip()
    name = name.replace(",", "")
    return name

