from typing import Any
from typing import Dict
from typing import List

from legacy.services.dataset_service import DataSetService
from legacy.services.utils import get_cosmos_service


def get_collections(
        cosmos_field_db_id: str,
        dataset_service: DataSetService,
        version: int = None
) -> List[Dict[str, Any]]:
    """
    Takes an ID to get its corresponding collection from Cosmos DB and returns it as a list.
    If a version is provided, returns collection for that version of the dataset, otherwise
    uses the latest dataset.

    :param cosmos_field_db_id: ID of a collection on Cosmos DB
    :type cosmos_field_db_id: str
    :param dataset_service: Dataset service to get latest version number
    :type dataset_service: DataSetService
    :param version: Provide a version to lookup a specific dataset, leave blank to use latest
    :type version: int
    :return: List of collections
    :rtype: List[Dict[str, Any]]
    """
    if not version:
        version = dataset_service.get_latest_version_number()
    cosmos_service = get_cosmos_service(cosmos_field_db_id)

    query = f"SELECT * from c where c.version = {version}"
    collections_list = list(cosmos_service.container.query_items(query, enable_cross_partition_query=True))

    return collections_list
