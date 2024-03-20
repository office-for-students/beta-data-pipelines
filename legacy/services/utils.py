"""Functions shared by Azure Functions"""

import html
import uuid
from typing import Any
from typing import Dict

from constants import COSMOS_DATABASE_ID
from constants import COSMOS_DATABASE_KEY
from constants import COSMOS_DATABASE_URI
from legacy.services.cosmosservice import CosmosService


def get_cosmos_service(container_id: str) -> CosmosService:
    """
    Creates and returns a cosmos service object with the appropriate credentials as specified
    in the environment variables.

    :param container_id: ID of container to create cosmos service for
    :type container_id: str
    :return: Cosmos service object
    :rtype: CosmosService
    """
    return CosmosService(
        cosmosdb_uri=COSMOS_DATABASE_URI,
        cosmosdb_key=COSMOS_DATABASE_KEY,
        database_id=COSMOS_DATABASE_ID,
        container_id=container_id
    )


def get_uuid() -> str:
    """
    Generates and returns a UUID.

    :return: Generated UUID
    :rtype: str
    """
    return str(uuid.uuid1())


def get_english_welsh_item(key: str, lookup_table: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes a key for an item and returns the English and Welsh values from the passed lookup table.

    :param key: Key to retrieve values with
    :type key: str
    :param lookup_table: Table to retrieve values from
    :type lookup_table: Dict[str, Any]
    :return: English and Welsh values as a dictionary
    :rtype: Dict[str, Any]
    """
    item = {}
    keyw = key + "W"
    if key in lookup_table:
        item["english"] = html.unescape(lookup_table[key])
    if keyw in lookup_table:
        item["welsh"] = html.unescape(lookup_table[keyw])
    return item
