"""Functions shared by Azure Functions"""

import html
import uuid
from typing import Any
from typing import Dict
from typing import List

import azure.cosmos.cosmos_client as cosmos_client
from decouple import config


def get_collection_link(collection_id: str) -> str:
    """Create and return collection link based on values passed in"""

    # Get the relevant properties from Application Settings
    cosmosdb_database_id = config("COSMOS_DATABASE_ID")
    cosmosdb_collection_id = config(collection_id)

    # Return a link to the relevant CosmosDB Container/Document Collection
    return "dbs/" + cosmosdb_database_id + "/colls/" + cosmosdb_collection_id


def sanitise_address_string(address_string: str) -> str:
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


def get_cosmos_client() -> cosmos_client.CosmosClient:
    cosmosdb_uri = config("COSMOS_DATABASE_URI")
    cosmosdb_key = config("COSMOS_DATABASE_KEY")

    master_key = "masterKey"

    return cosmos_client.CosmosClient(
        url=cosmosdb_uri, credential={master_key: cosmosdb_key}
    )


def get_uuid() -> str:
    return str(uuid.uuid1())


def get_ukrlp_lookups(version):
    """Returns a dictionary of UKRLP lookups"""

    cosmos_db_client = get_cosmos_client()

    collection_link = get_collection_link(
        collection_id="COSMOS_COLLECTION_INSTITUTIONS"
    )

    query = f"SELECT * from c WHERE c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    lookup_list = list(
        cosmos_db_client.QueryItems(collection_link, query, options)
    )

    # Previous data from the UKRLP smashed the ukprn number with the pub_ukprn,
    # to limit changes doing the same now.
    for lookup in lookup_list:
        lookup["institution"]["ukprn"] = lookup["institution"]["pub_ukprn"]

    return {
        lookup["institution"]["ukprn"]: {
            "ukprn_name": lookup["institution"]["pub_ukprn_name"],
            "ukprn_welsh_name": lookup["institution"]["pub_ukprn_welsh_name"]
        }
        for lookup in lookup_list
    }


def get_subject_lookups(version: int):
    """Returns a dictionary of UKRLP lookups"""

    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(
        collection_id="COSMOS_COLLECTION_SUBJECTS"
    )

    query = f"SELECT * from c WHERE c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    lookup_list = list(
        cosmos_db_client.QueryItems(collection_link, query, options)
    )

    return {lookup["code"]: lookup for lookup in lookup_list}


def get_courses_by_version(version: int) -> List:
    """Returns a dictionary of courses for a version of the dataset"""

    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(
        collection_id="COSMOS_COLLECTION_COURSES"
    )

    query = f"SELECT * from c WHERE c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    course_list = list(
        cosmos_db_client.QueryItems(collection_link, query, options)
    )

    return course_list


def get_english_welsh_item(key, lookup_table: Dict) -> Dict[str, Any]:
    item = {}
    keyw = key + "W"
    if key in lookup_table:
        item["english"] = html.unescape(lookup_table[key])
    if keyw in lookup_table:
        item["welsh"] = html.unescape(lookup_table[keyw])
    return item
