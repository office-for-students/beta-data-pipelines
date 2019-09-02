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
        dataset_doc = self.get_latest_dataset_doc()
        if item == "root":
            dataset_doc["status"] = value
        elif item == "courses":
            dataset_doc["builds"]["courses"]["status"] = value
        elif item == "institutions":
            dataset_doc["builds"]["institutions"]["status"] = value
        elif item == "search":
            dataset_doc["builds"]["search"]["status"] = value
        self.cosmos_client.UpsertItem(self.collection_link, dataset_doc)

    def get_latest_dataset_doc(self):
        max_version_number = self.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {max_version_number}"
        options = {"enableCrossPartitionQuery": True}
        return list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )[0]


dsh = DataSetHelper()
dsh.update_status("institutions", "in progress")
