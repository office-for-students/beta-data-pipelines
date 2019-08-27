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


class DataSetCreator:
    def __init__(self):
        self.cosmos_client = get_cosmos_client()
        self.collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbDataSetCollectionId"
        )

    def create_dataset_doc(self):
        version_number = self.get_next_dataset_version_number()
        print(version_number)

    def get_next_dataset_version_number(self):
        if self.get_number_of_dataset_docs() == 0:
            return 1

    def get_number_of_dataset_docs(self):
        query = "SELECT * from c "

        options = {"enableCrossPartitionQuery": True}

        data_set_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return len(data_set_list)


dsc = DataSetCreator()
dsc.create_dataset_doc()
