import logging
from datetime import datetime
from typing import Any

from services.dataset_service.base import DataSetServiceBase


class DataSetService(DataSetServiceBase):

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
        dataset_doc["updated_at"] = datetime.utcnow().isoformat()
        if updated_at:
            dataset_doc["updated_at"] = updated_at
        self.container.upsert_item(dataset_doc)
        logging.info(
            f"DataSetService: updated '{item}' to '{value}' for "
            f"DataSet version {dataset_doc['version']}"
        )

    def get_latest_doc(self) -> dict[str, Any]:
        """
        Retrieves the latest dataset document from the database

        :return: Dictionary containing latest dataset document data
        :rtype: Dict[str, Any]
        """
        latest_version_number = self.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {latest_version_number}"
        the_list = list(self.container.query_items(query, enable_cross_partition_query=True))
        the_list_item = the_list[0]
        return the_list_item

    def get_latest_version_number(self) -> int:
        """
        Gets the latest version of the dataset from the database

        :return: Version number of the latest dataset
        :rtype: int
        """
        query = "SELECT VALUE MAX(c.version) from c "
        max_version_number_list = list(
            self.container.query_items(query, enable_cross_partition_query=True))
        return max_version_number_list[0]

    def query_items(self, query: str) -> list[dict[str, Any]]:
        """
        Performs the passed query on the database with "enableCrossPartitionQuery" set to true

        :param query: Query to run on the database
        :return: List of dictionaries containing the query results
        """
        return list(self.container.query_items(query, enable_cross_partition_query=True))

    def create_item(self, dataset_doc: dict[str, Any]) -> None:
        """
        Creates an item on the database with the passed dataset document.

        :param dataset_doc: Dataset to create a database item with
        :return: None
        """
        self.container.create_item(dataset_doc)

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