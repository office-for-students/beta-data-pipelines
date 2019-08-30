import inspect
import logging
import os
import sys

from datetime import datetime, timezone
from dateutil import parser

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from SharedCode.utils import get_cosmos_client, get_collection_link

from SharedCode.exceptions import DataSetTooEarlyError


class DataSetCreator:
    def __init__(self):
        self.cosmos_client = get_cosmos_client()
        self.collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbDataSetCollectionId"
        )

    def load_new_dataset_doc(self):
        if not self.has_enough_time_elaspsed_since_last_dataset_created():
            raise DataSetTooEarlyError

        dataset_doc = self.get_next_dataset_doc()
        self.cosmos_client.CreateItem(self.collection_link, dataset_doc)
        logging.info(f"Created new vertsion {dataset_doc['version']} DataSet")

    def has_enough_time_elaspsed_since_last_dataset_created(self):
        dt_of_latest_dataset_doc = self.get_datetime_of_latest_dataset_doc()
        time_in_minutes_since_latest_dataset_doc = get_time_in_minutes_since_given_datetime(
            dt_of_latest_dataset_doc
        )
        time_in_minutes_to_wait = int(
            os.environ["TimeInMinsToWaitBeforeCreateNewDataSet"]
        )
        if time_in_minutes_to_wait > time_in_minutes_since_latest_dataset_doc:
            return False
        return True

    def get_datetime_of_latest_dataset_doc(self):
        max_version_number = self.get_max_version_number()
        query = f"SELECT * FROM c WHERE c.version = {max_version_number}"
        options = {"enableCrossPartitionQuery": True}
        latest_doc = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )[0]
        created_at_str = latest_doc["created_at"]
        dt_created_at = parser.isoparse(created_at_str)
        dt_created_at.replace(tzinfo=timezone.utc)
        return dt_created_at

    def get_max_version_number(self):
        query = "SELECT VALUE MAX(c.version) from c "
        options = {"enableCrossPartitionQuery": True}
        max_version_number_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return max_version_number_list[0]

    def get_next_dataset_doc(self):
        next_version_number = self.get_next_dataset_version_number()
        dataset_doc = {}
        dataset_doc["builds"] = self.get_builds_value()
        dataset_doc["created_at"] = datetime.now(timezone.utc).isoformat()
        dataset_doc["is_published"] = False
        dataset_doc["status"] = "in progress"
        dataset_doc["version"] = next_version_number
        return dataset_doc

    def get_builds_value(self):
        builds = {}
        builds["courses"] = get_initial_build_value()
        builds["institutions"] = get_initial_build_value()
        builds["search"] = get_initial_build_value()
        return builds

    def get_next_dataset_version_number(self):
        if self.get_number_of_dataset_docs() == 0:
            return 1
        return self.get_max_version_number() + 1

    def get_number_of_dataset_docs(self):
        query = "SELECT * FROM c "
        options = {"enableCrossPartitionQuery": True}
        data_set_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return len(data_set_list)


def get_time_in_minutes_since_given_datetime(dt_in_the_past):
    dt_now = datetime.now(timezone.utc)
    minutes_diff = round((dt_now - dt_in_the_past).total_seconds() / 60)
    return minutes_diff


def get_initial_build_value():
    return {"status": "pending"}
