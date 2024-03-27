from typing import Any
from typing import Dict
from typing import List

from services.cosmosservice import CosmosService
from services.dataset_service import DataSetService

def get_collections(
        cosmos_service: CosmosService,
        dataset_service: DataSetService,
        collection_id: str = None,
        version: int = None
) -> List[Dict[str, Any]]:
    """
    Takes an ID to get its corresponding collection from Cosmos DB and returns it as a list.
    If a version is provided, returns collection for that version of the dataset, otherwise
    uses the latest dataset.

    :param collection_id:
    :param cosmos_service:
    :param dataset_service: Dataset service to get latest version number
    :type dataset_service: DataSetService
    :param version: Provide a version to look up a specific dataset, leave blank to use latest
    :type version: int
    :return: List of collections
    :rtype: List[Dict[str, Any]]
    """
    if collection_id is None:
        raise ValueError("Please provide a collection ID")

    if not version:
        version = dataset_service.get_latest_version_number()

    query = f"SELECT * from c where c.version = {version}"
    container = cosmos_service.get_container(container_id=collection_id)
    collections_list = list(container.query_items(query=query, enable_cross_partition_query=True))

    return collections_list
