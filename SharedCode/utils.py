"""Functions shared by Azure Functions"""

import os
import uuid

import azure.cosmos.cosmos_client as cosmos_client


def get_collection_link(db_id, collection_id):
    """Create and return collection link based on values passed in"""

    # Get the relevant properties from Application Settings
    cosmosdb_database_id = os.environ[db_id]
    cosmosdb_collection_id = os.environ[collection_id]

    #Return a link to the relevant CosmosDB Container/Document Collection
    return 'dbs/' + cosmosdb_database_id + '/colls/' + cosmosdb_collection_id


def get_cosmos_client():
    cosmosdb_uri = os.environ['AzureCosmosDbUri']
    cosmosdb_key = os.environ['AzureCosmosDbKey']

    master_key = 'masterKey'

    return cosmos_client.CosmosClient(url_connection=cosmosdb_uri,
                                      auth={master_key: cosmosdb_key})

def get_uuid():
    return str(uuid.uuid1())

def get_ukrlp_lookups():
    """Returns a dictionary of UKRLP lookups"""

    cosmos_client = get_cosmos_client()
    collection_link = get_collection_link(
                'AzureCosmosDbDatabaseId', 'AzureCosmosDbUkRlpCollectionId')

    query = ( "SELECT * from c ")

    options = {'enableCrossPartitionQuery': True}

    lookup_list = list(
        cosmos_client.QueryItems(collection_link, query, options))

    return {lookup['ukprn']:lookup for lookup in lookup_list}
