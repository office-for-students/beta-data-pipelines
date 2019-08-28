import datetime
import inspect
import json
import os
import sys

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from SharedCode.utils import get_cosmos_client, get_collection_link


class DataSetCreator:
    def __init__(self):
        self.cosmos_client = get_cosmos_client()
        self.collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbDataSetCollectionId"
        )

    def load_new_dataset_doc(self):
        dataset_doc = self.get_latest_dataset_doc()
        dsd = json.dumps(dataset_doc)
        print(f"data set doc {dsd}")
        self.cosmos_client.CreateItem(self.collection_link, dataset_doc)

    def get_latest_dataset_doc(self):
        next_version_number = self.get_next_dataset_version_number()
        dataset_doc = {}
        dataset_doc["builds"] = self.get_builds_value()
        dataset_doc["created_at"] = datetime.datetime.utcnow().isoformat()
        dataset_doc["is_published"] = False
        dataset_doc["status"] = "pending"
        dataset_doc["version"] = next_version_number
        return dataset_doc

    def get_builds_value(self):
        builds = {}
        builds["courses"] = self.get_initial_build_value()
        builds["institutions"] = self.get_initial_build_value()
        builds["search"] = self.get_initial_build_value()
        return builds

    def get_initial_build_value(self):
        return {"error": "", "status": "pending"}

    def get_next_dataset_version_number(self):
        if self.get_number_of_dataset_docs() == 0:
            return 1
        return self.get_max_version_number() + 1

    def get_max_version_number(self):
        query = "SELECT VALUE MAX(c.version) from c "
        options = {"enableCrossPartitionQuery": True}
        max_version_number_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return max_version_number_list[0]

    def get_number_of_dataset_docs(self):
        query = "SELECT * from c "
        options = {"enableCrossPartitionQuery": True}
        data_set_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return len(data_set_list)


# TODO remove
#dsc = DataSetCreator()
#dsc.load_latest_dataset_doc()
