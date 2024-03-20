"""Functions shared by Azure Functions"""

import html
import uuid
from typing import Any
from typing import Dict

from azure.cosmos import CosmosClient

from constants import COSMOS_DATABASE_ID
from constants import COSMOS_DATABASE_KEY
from constants import COSMOS_DATABASE_URI
from constants import KEY_COSMOS_MASTER_KEY


def get_collection_link(collection_id: str) -> str:
    """
    Create and return collection link based on values passed in

    :param collection_id: ID of collection to return link of
    :type collection_id: str
    :return: Collection link
    :rtype: str
    """

    # Return a link to the relevant CosmosDB Container/Document Collection
    return "dbs/" + COSMOS_DATABASE_ID + "/colls/" + collection_id


def get_cosmos_client() -> CosmosClient:
    """
    Creates and returns a cosmos client object with the appropriate credentials as specified in the environment variables

    :return: Cosmos client object
    :rtype: CosmosClient
    """
    return CosmosClient(url=COSMOS_DATABASE_URI, credential={KEY_COSMOS_MASTER_KEY: COSMOS_DATABASE_KEY})


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
