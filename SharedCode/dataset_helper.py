import inspect
import os
import sys


CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from SharedCode.utils import get_cosmos_client, get_collection_link


class DataSetHelper:
    def __init__(self):
        self.cosmos_client = get_cosmos_client()
        print(f"init cosmos_client type {type(self.cosmos_client)}")
        self.collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbDataSetCollectionId"
        )

    def get_latest_version_number(self):
        query = "SELECT VALUE MAX(c.version) from c "
        options = {"enableCrossPartitionQuery": True}
        max_version_number_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return max_version_number_list[0]

    def update_status(self, item, value):
        dataset_doc = self.get_latest_doc()
        if item == "root":
            dataset_doc["status"] = value
        else:
            dataset_doc["builds"][item]["status"] = value
        self.cosmos_client.UpsertItem(self.collection_link, dataset_doc)

    def get_updated_status_doc(self, item, value):
        dataset_doc = self.get_latest_doc()
        if item == "root":
            dataset_doc["status"] = value
        else:
            dataset_doc["builds"][item]["status"] = value
        self.cosmos_client.UpsertItem(self.collection_link, dataset_doc)

    def get_latest_doc(self):
        max_version_number = self.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {max_version_number}"
        options = {"enableCrossPartitionQuery": True}
        print(f"cosmos_client type {type(self.cosmos_client)}")
        return list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )[0]


