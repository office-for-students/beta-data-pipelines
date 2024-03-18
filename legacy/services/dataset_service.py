import datetime
import inspect
import logging
import os
import sys
from typing import Any
from typing import Dict
from typing import List

from .utils import get_collection_link
from .utils import get_cosmos_client

CURRENT_DIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)


class DataSetService:
    def __init__(self) -> None:
        logging.info("Init for DataSetService")
        self.cosmos_client = get_cosmos_client()
        self.collection_link = get_collection_link("COSMOS_COLLECTION_DATASET")

    def update_status(self, item: str, value: str, updated_at: str = None) -> None:
        """
        Updates the status of the dataset.
        Takes an item (e.g., 'root', 'institutions', 'courses', etc.) and a new value (i.e., the current status).

        :param item: Item of dataset to update status of
        :type item: str
        :param value: New status to be updated to
        :type value: str
        :param updated_at: Date at which the status is updated at
        :type updated_at: str
        :return: None
        """
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

    def get_latest_doc(self) -> Dict[str, Any]:
        """
        Retrieves the latest dataset document from the database

        :return: Dictionary containing latest dataset document data
        :rtype: Dict[str, Any]
        """
        latest_version_number = self.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {latest_version_number}"
        options = {"enableCrossPartitionQuery": True}
        the_list = list(self.cosmos_client.QueryItems(self.collection_link, query, options))
        the_list_item = the_list[0]
        return the_list_item

    def get_latest_version_number(self) -> int:
        """
        Gets the latest version of the dataset from the database

        :return: Version number of the latest dataset
        :rtype: int
        """
        query = "SELECT VALUE MAX(c.version) from c "
        options = {"enableCrossPartitionQuery": True}
        max_version_number_list = list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )
        return max_version_number_list[0]

    def query_items(self, query: str) -> List[Dict[str, Any]]:
        """
        Performs the passed query on the database with "enableCrossPartitionQuery" set to true

        :param query: Query to run on the database
        :return: List of dictionaries containing the query results
        """
        options = {"enableCrossPartitionQuery": True}
        return list(
            self.cosmos_client.QueryItems(self.collection_link, query, options)
        )

    def create_item(self, dataset_doc: Dict[str, Any]) -> None:
        """
        Creates an item on the database with the passed dataset document.

        :param dataset_doc: Dataset to create a database item with
        :return: None
        """
        self.cosmos_client.CreateItem(self.collection_link, dataset_doc)

    def have_all_builds_succeeded(self) -> bool:
        """
        Checks whether all dataset builds have succeeded. Returns False if at least one has not succeeded, else True

        :return: True if all dataset builds have succeeded, else False
        :rtype: bool
        """
        dataset_doc = self.get_latest_doc()
        build_statuses = [
            dataset_doc["builds"][item]["status"] == "succeeded"
            for item in ("courses", "institutions", "search", "subjects")
        ]
        return all(build_statuses)
