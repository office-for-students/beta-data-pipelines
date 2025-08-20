"""Functions shared by Azure Functions"""

import html
import os
import uuid
from typing import Dict


import azure.cosmos.cosmos_client as cosmos_client


def get_collection_link(db_id, collection_id):
    """Create and return collection link based on values passed in"""


    # Return a link to the relevant CosmosDB Container/Document Collection
    return "dbs/" + db_id + "/colls/" + collection_id


def sanitise_address_string(address_string):
    cleaned = address_string.replace(",,", ",")
    as_array = cleaned.split(',')

    final = []
    for a in as_array:
        final.append(a.rstrip())

    sanitised = ','.join(final)

    return sanitised


def normalise_url(website_url: str) -> str:
    if website_url == "":
        return ""
    params = website_url.split("://")
    if len(params) == 1:
        return f"https://{website_url.rstrip()}"
    else:
        return f"https://{params[1].rstrip()}"


def get_cosmos_client():
    cosmosdb_uri = os.environ["AzureCosmosDbUri"]
    cosmosdb_key = os.environ["AzureCosmosDbKey"]

    return cosmos_client.CosmosClient(
        url=cosmosdb_uri, credential=cosmosdb_key
    )


def get_uuid():
    return str(uuid.uuid1())


def get_ukrlp_lookups(version):
    """Returns a dictionary of UKRLP lookups"""

    cosmos_db_client = get_cosmos_client()
    db_id = os.environ.get("AzureCosmosDbDatabaseId")
    collection_id = os.environ.get("AzureCosmosDbInstitutionsCollectionId")

    database = cosmos_db_client.get_database_client(db_id)
    container = database.get_container_client(collection_id)

    query = f"SELECT * from c WHERE c.version = {version}"


    lookup_list = list(
        container.query_items(query=query, enable_cross_partition_query=True)
    )

    # Previous data from the UKRLP smashed the ukprn number with the pub_ukprn,
    # to limit changes doing the same now.
    for lookup in lookup_list:
        lookup["institution"]["ukprn"] = lookup["institution"]["pub_ukprn"]

    return {lookup["institution"]["ukprn"]: {
        "ukprn_name": lookup["institution"]["pub_ukprn_name"],
        "ukprn_welsh_name": lookup["institution"]["pub_ukprn_welsh_name"]
    } for lookup in lookup_list}


def get_subject_lookups(version):
    """Returns a dictionary of UKRLP lookups"""

    cosmos_db_client = get_cosmos_client()
    db_id = os.environ.get("AzureCosmosDbDatabaseId")
    collection_id = os.environ.get("AzureCosmosDbSubjectsCollectionId")

    database = cosmos_db_client.get_database_client(db_id)
    container = database.get_container_client(collection_id)

    query = f"SELECT * from c WHERE c.version = {version}"

    options = {"enable_cross_partition_query": True}

    lookup_list = list(
        container.query_items(query=query, **options)
    )

    return {lookup["code"]: lookup for lookup in lookup_list}


def get_courses_by_version(version):
    """Returns a dictionary of courses for a version of the dataset"""
    cosmos_db_client = get_cosmos_client()
    database_id = os.environ.get("AzureCosmosDbDatabaseId")
    container_id = os.environ.get("AzureCosmosDbCoursesCollectionId")

    database = cosmos_db_client.get_database_client(database_id)
    container = database.get_container_client(container_id)

    query = f"SELECT * from c WHERE c.version = {version}"

    options = {"enable_cross_partition_query": True}

    course_list = list(
        container.query_items(query=query, **options)
    )

    return course_list


def get_english_welsh_item(key, lookup_table: Dict):
    item = {}
    keyw = key + "W"
    if key in lookup_table:
        item["english"] = html.unescape(lookup_table[key])
    if keyw in lookup_table:
        item["welsh"] = html.unescape(lookup_table[keyw])
    return item