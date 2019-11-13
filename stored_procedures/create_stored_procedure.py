"""Adds a stored procedure for importing bulk data against a cosmos db collection

Reads spBulkCreate.js file and stores procedure onto configured collection

"""

import os
import azure.cosmos.cosmos_client as cosmos_client


def create_stored_procedure():
    cosmosdb_client = get_cosmos_client()

    collection_link = get_collection_link(
        "AzureCosmosDbDatabaseId", "AzureCosmosDbCollectionId"
    )

    with open("./spBulkCreate.js") as file:
        file_contents = file.read()

        sproc_definition = {"id": "bulkImport", "serverScript": file_contents}
        cosmosdb_client.CreateStoredProcedure(collection_link, sproc_definition)


def get_cosmos_client():
    cosmosdb_uri = os.environ["AzureCosmosDbUri"]
    cosmosdb_key = os.environ["AzureCosmosDbKey"]

    master_key = "masterKey"

    return cosmos_client.CosmosClient(
        url_connection=cosmosdb_uri, auth={master_key: cosmosdb_key}
    )


def get_collection_link(db_id, collection_id):
    """Create and return collection link based on values passed in"""

    # Get the relevant properties from Application Settings
    cosmosdb_database_id = os.environ[db_id]
    cosmosdb_collection_id = os.environ[collection_id]

    # Return a link to the relevant CosmosDB Container/Document Collection
    return "dbs/" + cosmosdb_database_id + "/colls/" + cosmosdb_collection_id


if __name__ == "__main__":
    create_stored_procedure()
