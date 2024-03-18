from typing import Any
from typing import Dict
from typing import List

from legacy.services.dataset_service import DataSetService
from legacy.services.utils import get_collection_link
from legacy.services.utils import get_cosmos_client


def get_collections(cosmos_field_db_id: str) -> List[Dict[str, Any]]:
    """
    Takes an ID to get its corresponding collection from Cosmos DB and returns it as a list

    :param cosmos_field_db_id: ID of a collection on Cosmos DB
    :type cosmos_field_db_id: str
    :return: List of collections
    :rtype: List[Dict[str, Any]]
    """
    version = DataSetService().get_latest_version_number()
    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(cosmos_field_db_id)

    query = f"SELECT * from c where c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    collections_list = list(cosmos_db_client.QueryItems(collection_link, query, options))

    return collections_list


def get_institutions() -> List[Dict[str, Any]]:
    """
    Gets all the institutions from the Cosmos DB as a list of dictionaries

    :return: List of institutions from Cosmos DB
    :rtype: List[Dict[str, Any]]
    """
    return get_collections("COSMOS_COLLECTION_INSTITUTIONS")

