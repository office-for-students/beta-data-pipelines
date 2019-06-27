import os
import uuid

import azure.cosmos.cosmos_client as cosmos_client


def get_cosmos_client():
    # TODO run this over TLS
    cosmosdb_uri = os.environ['AzureCosmosDbUri']
    cosmosdb_key = os.environ['AzureCosmosDbKey']
    master_key = 'masterKey'

    return cosmos_client.CosmosClient(url_connection=cosmosdb_uri, auth={master_key: cosmosdb_key})

def get_uuid():
    id = uuid.uuid1()
    return str(id.hex)