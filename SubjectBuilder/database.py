import logging
import requests
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.documents as documents
import time

from . import models


def load_collection(url, api_key, db_id, collection_id, rows, version):
    loader = Loader(url, api_key, db_id, collection_id, rows, version)

    loader.load_subject_documents()


class Loader:
    def __init__(self, cosmosdb_uri, cosmos_key, db_id, collection_id, rows, version):

        master_key = "masterKey"

        self.cosmos_db_client = cosmos_client.CosmosClient(
            url_connection=cosmosdb_uri, auth={master_key: cosmos_key}
        )

        self.collection_link = "dbs/" + db_id + "/colls/" + collection_id
        self.db_id = db_id
        self.collection_id = collection_id
        self.rows = rows
        self.version = version

    def load_subject_documents(self):
        options = {"partitionKey": str(self.version)}
        sproc_link = self.collection_link + "/sprocs/bulkImport"

        subject_count = 0
        new_docs = []
        sproc_count = 0

        for row in self.rows:
            subject_count += 1
            sproc_count += 1

            if subject_count == 1:
                logging.info("skipping header row")
                continue

            # Transform row into json object
            new_docs.append(models.build_subject_doc(row, self.version))

            if sproc_count == 100:
                logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
                self.cosmos_db_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
                logging.info(f"Successfully loaded another {sproc_count} documents")
                # Reset values
                new_docs = []
                sproc_count = 0
                time.sleep(10)

        if sproc_count > 0:
            logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
            self.cosmos_db_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
            logging.info(f"Successfully loaded another {sproc_count} documents")

        logging.info(
            f"loaded {subject_count - 1} into {self.collection_id} collection in {self.db_id} database"
        )
