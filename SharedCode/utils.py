"""Functions shared by Azure Functions"""

import os
import html
import uuid

import azure.cosmos.cosmos_client as cosmos_client


def get_collection_link(db_id, collection_id):
    """Create and return collection link based on values passed in"""

    # Get the relevant properties from Application Settings
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


def get_uuid():
    return str(uuid.uuid1())


def get_ukrlp_lookups():
    """Returns a dictionary of UKRLP lookups"""

    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(
        "AzureCosmosDbDatabaseId", "AzureCosmosDbUkRlpCollectionId"
    )

    query = "SELECT * from c "

    options = {"enableCrossPartitionQuery": True}

    lookup_list = list(
        cosmos_db_client.QueryItems(collection_link, query, options)
    )

    return {lookup["ukprn"]: lookup for lookup in lookup_list}


def get_courses_by_version(version):
    """Returns a dictionary of courses for a version of the dataset"""

    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(
        "AzureCosmosDbDatabaseId", "AzureCosmosDbCoursesCollectionId"
    )

    query = f"SELECT * from c WHERE c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    course_list = list(
        cosmos_db_client.QueryItems(collection_link, query, options)
    )

    return course_list


def get_english_welsh_item(key, lookup_table):
    item = {}
    keyw = key + "W"
    if key in lookup_table:
        item["english"] = html.unescape(lookup_table[key])
    if keyw in lookup_table:
        item["welsh"] = html.unescape(lookup_table[keyw])
    return item
