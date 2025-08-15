import datetime
import inspect
import logging
import os
import sys

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from SharedCode.utils import get_cosmos_client


class DataSetHelper:
    def __init__(self):
        logging.info("Init for DataSetHelper")
        self.cosmos_client = get_cosmos_client()
        self.database_id = os.environ["AzureCosmosDbDatabaseId"]
        self.container_id = os.environ["AzureCosmosDbDataSetCollectionId"]
        self.database = self.cosmos_client.get_database_client(self.database_id)
        self.container = self.database.get_container_client(self.container_id)

    def update_status(self, item, value, updated_at=None):
        dataset_doc = self.get_latest_doc()
        if item == "root":
            dataset_doc["status"] = value
        else:
            dataset_doc["builds"][item]["status"] = value
        dataset_doc["updated_at"] = datetime.datetime.utcnow().isoformat()
        if updated_at:
            dataset_doc["updated_at"] = updated_at

        self.container.upsert_item(dataset_doc)
        logging.info(
            f"DataSetHelper: updated '{item}' to '{value}' for "
            f"DataSet version {dataset_doc['version']}"
        )

    def get_latest_doc(self):
        latest_version_number = self.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {latest_version_number}"
        options = {"enable_cross_partition_query": True}
        the_list = list(self.container.query_items(query=query, **options))
        the_list_item = the_list[0]
        return the_list_item

    def get_latest_version_number(self):
        query = 'SELECT VALUE MAX(c.version) from c '
        max_version_number_list = list(
            self.container.query_items(query=query, enable_cross_partition_query=True)
        )
        return max_version_number_list[0]

    def query_items(self, query):
        return list(
            self.container.query_items(query=query, enable_cross_partition_query=True)
        )

    def create_item(self, dataset_doc):
        self.container.create_item(body=dataset_doc)

    def have_all_builds_succeeded(self):
        dataset_doc = self.get_latest_doc()
        build_statuses = [
            dataset_doc["builds"][item]["status"] == "succeeded"
            for item in ("courses", "institutions", "search", "subjects")
        ]
        return all(build_statuses)