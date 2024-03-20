from typing import Any
from typing import Dict
from typing import List

from constants import COSMOS_DATABASE_ID
from legacy.services.dataset_service import DataSetService
from legacy.services.utils import get_cosmos_client


def get_collections(cosmos_field_db_id: str, version: int = None) -> List[Dict[str, Any]]:
    """
    Takes an ID to get its corresponding collection from Cosmos DB and returns it as a list.
    If a version is provided, returns collection for that version of the dataset, otherwise
    uses the latest dataset.

    :param cosmos_field_db_id: ID of a collection on Cosmos DB
    :type cosmos_field_db_id: str
    :param version: Provide a version to lookup a specific dataset, leave blank to use latest
    :type version: int
    :return: List of collections
    :rtype: List[Dict[str, Any]]
    """
    if not version:
        version = DataSetService().get_latest_version_number()
    cosmos_client = get_cosmos_client()
    cosmos_db_client = cosmos_client.get_database_client(COSMOS_DATABASE_ID)
    cosmos_container_client = cosmos_db_client.get_container_client(cosmos_field_db_id)

    query = f"SELECT * from c where c.version = {version}"
    collections_list = list(cosmos_container_client.query_items(query, enable_cross_partition_query=True))

    return collections_list
