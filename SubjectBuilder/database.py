import logging
import requests
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.documents as documents

from . import models


def build_collection(url, api_key, throughput, db_id, collection_id):
        collection = Collection(url, api_key, throughput, db_id, collection_id)

        collection.delete_if_already_exists()
        collection.create()


def load_collection(url, api_key, db_id, collection_id, rows):
        loader = Loader(url, api_key, db_id, collection_id, rows)

        loader.load_subject_documents()


class Collection:
    """Creates or overwrites existing collection"""

    def __init__(self, cosmosdb_uri, cosmos_key, throughput, db_id, collection_id):

        master_key = "masterKey"

        self.cosmos_db_client = cosmos_client.CosmosClient(
            url_connection=cosmosdb_uri, auth={master_key: cosmos_key}
        )

        self.db_id = db_id
        self.collection_id = collection_id
        self.throughput = throughput

    def delete_if_already_exists(self):
        try:
            collection_link = "dbs/" + self.db_id + "/colls/" + self.collection_id
            self.cosmos_db_client.DeleteContainer(collection_link)

        except errors.HTTPFailure as e:
            if e.status_code == 404:
                logging.warning(
                    f"no collection to delete by the name of {self.collection_id}"
                )
            else:
                logging.exception(
                    f"unexpected error deleting collection\ndatabase: {self.db_id}\ncollection: {self.collection_id}",
                    exc_info=True,
                )
                raise

    def create(self):
        try:
            database_link = "dbs/" + self.db_id
            collection_definition = {
                "id": self.collection_id,
                "partitionKey": {
                    "paths": ["/id"],
                    "kind": documents.PartitionKind.Hash,
                },
            }

            options = {"offerThroughput": self.throughput}

            self.cosmos_db_client.CreateContainer(
                database_link, collection_definition, options
            )

        except errors.HTTPFailure as e:
            if e.status_code == 409:
                logging.exception(
                    f"collection already exists by the name of {self.collection_id}\nLikely a race condition with another instance?"
                )
                raise

            logging.exception(
                f"unexpected error creating collection\ndatabase: {self.db_id}\ncollection: {self.collection_id}",
                exc_info=True,
            )
            raise


class Loader:
    def __init__(self, cosmosdb_uri, cosmos_key, db_id, collection_id, rows):

        master_key = "masterKey"

        self.cosmos_db_client = cosmos_client.CosmosClient(
            url_connection=cosmosdb_uri, auth={master_key: cosmos_key}
        )

        self.collection_link = "dbs/" + db_id + "/colls/" + collection_id
        self.db_id = db_id
        self.collection_id = collection_id
        self.rows = rows

    def load_subject_documents(self):
        subject_count = 0
        for row in self.rows:
            subject_count += 1

            if subject_count == 1:
                logging.info("skipping header row")
                continue

            # Transform row into json object
            subject_doc = models.build_subject_doc(row)

            # Send to cosmos db
            self.cosmos_db_client.CreateItem(self.collection_link, subject_doc)

        logging.info(
            f"loaded {subject_count - 1} into {self.collection_id} collection in {self.db_id} database"
        )
