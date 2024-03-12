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

from .utils import get_cosmos_client, get_collection_link


class DataSetService:
    def __init__(self):
        logging.info("Init for DataSetService")
        self.cosmos_client = get_cosmos_client()
        self.collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbDataSetCollectionId"
        )

    def update_status(self, item, value, updated_at=None):
        dataset_doc = self.get_latest_doc()
        if item == "root":
            dataset_doc["status"] = value
        else:
            dataset_doc["builds"][item]["status"] = value
        dataset_doc["updated_at"] = datetime.datetime.utcnow().isoformat()
        if updated_at:
            dataset_doc["updated_at"] = updated_at
        self.cosmos_client.UpsertItem(self.collection_link, dataset_doc)
        logging.info(
            f"DataSetService: updated '{item}' to '{value}' for "
            f"DataSet version {dataset_doc['version']}"
        )

    def get_latest_doc(self):
        latest_version_number = self.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {latest_version_number}"
        options = {"enableCrossPartitionQuery": True}
        the_list = list(self.cosmos_client.QueryItems(self.collection_link, query, options))
        the_list_item = the_list[0]
        return the_list_item

    def get_latest_version_number(self):
        query = "SELECT VALUE MAX(c.version) from c "
        options = {"enableCrossPartitionQuery": True}
        max_version_number_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return max_version_number_list[0]

    def query_items(self, query):
        options = {"enableCrossPartitionQuery": True}
        return list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )

    def create_item(self, dataset_doc):
        self.cosmos_client.CreateItem(self.collection_link, dataset_doc)

    def have_all_builds_succeeded(self):
        dataset_doc = self.get_latest_doc()
        build_statuses = [
            dataset_doc["builds"][item]["status"] == "succeeded"
            for item in ("courses", "institutions", "search", "subjects")
        ]
        return all(build_statuses)
