"""Functions shared by Azure Functions"""

import html
import uuid
from typing import Any
from typing import Dict
from typing import List

from azure.cosmos import CosmosClient

from constants import COSMOS_COLLECTION_COURSES
from constants import COSMOS_COLLECTION_INSTITUTIONS
from constants import COSMOS_COLLECTION_SUBJECTS
from constants import COSMOS_DATABASE_ID
from constants import COSMOS_DATABASE_KEY
from constants import COSMOS_DATABASE_URI


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


def sanitise_address_string(address_string: str) -> str:
    """
    Sanitises passed address string by removing leading and trailing whitespace and removing extra commas

    :param address_string: Address string to sanitise
    :type address_string: str
    :return: Cleaned address string
    :rtype: str
    """
    cleaned = address_string.replace(",,", ",")
    as_array = cleaned.split(',')

    final = []
    for a in as_array:
        final.append(a.rstrip())

    sanitised = ','.join(final)

    return sanitised


def normalise_url(website_url: str) -> str:
    """
    Normalises a website URL by removing trailing whitespace and adding "https://".
    Returns an empty string if an empty string is provided.

    :param website_url: Website URL string to normalise
    :type website_url: str
    :return: Normalized website URL
    :rtype: str
    """
    if website_url == "":
        return ""
    params = website_url.split("://")
    if len(params) == 1:
        return f"https://{website_url.rstrip()}"
    else:
        return f"https://{params[1].rstrip()}"


def get_cosmos_client() -> CosmosClient:
    """
    Creates and returns a cosmos client object with the appropriate credentials as specified in the environment variables

    :return: Cosmos client object
    :rtype: CosmosClient
    """
    master_key = "masterKey"

    return CosmosClient(url=COSMOS_DATABASE_URI, credential={master_key: COSMOS_DATABASE_KEY})


def get_uuid() -> str:
    """
    Generates and returns a UUID.

    :return: Generated UUID
    :rtype: str
    """
    return str(uuid.uuid1())


def get_ukrlp_lookups(version: int) -> Dict[str, Any]:
    """
    Returns a dictionary of UKRLP lookups, including English and Welsh institution names.

    :param version: Dataset version to perform UKRLP lookups on
    :type version: int
    :return: UKRLP lookups as a dictionary
    :rtype: Dict[str, Any]
    """

    cosmos_client = get_cosmos_client()
    cosmos_db_client = cosmos_client.get_database_client(COSMOS_DATABASE_ID)
    cosmos_container_client = cosmos_db_client.get_container_client(COSMOS_COLLECTION_INSTITUTIONS)

    query = f"SELECT * from c WHERE c.version = {version}"

    lookup_list = list(cosmos_container_client.query_items(query))

    # Previous data from the UKRLP smashed the ukprn number with the pub_ukprn,
    # to limit changes doing the same now.
    for lookup in lookup_list:
        lookup["institution"]["ukprn"] = lookup["institution"]["pub_ukprn"]

    return {
        lookup["institution"]["ukprn"]: {
            "ukprn_name": lookup["institution"]["pub_ukprn_name"],
            "ukprn_welsh_name": lookup["institution"]["pub_ukprn_welsh_name"]
        }
        for lookup in lookup_list
    }


def get_subject_lookups(version: int) -> Dict[str, Any]:
    """
    Returns a dictionary of subject lookups, including the subject code.

    :param version: Version of dataset to perform lookup on
    :type version: str
    :return: Dictionary of subject lookups containing subject and code
    :rtype: Dict[str, Any]
    """

    cosmos_client = get_cosmos_client()
    cosmos_db_client = cosmos_client.get_database_client(COSMOS_DATABASE_ID)
    cosmos_container_client = cosmos_db_client.get_container_client(COSMOS_COLLECTION_SUBJECTS)

    query = f"SELECT * from c WHERE c.version = {version}"
    lookup_list = list(cosmos_container_client.query_items(query, enable_cross_partition_query=True))

    return {lookup["code"]: lookup for lookup in lookup_list}


def get_courses_by_version(version: int) -> List[Dict[str, Any]]:
    """
    Returns a dictionary of courses for a version of the dataset.

    :param version: Version of dataset to perform lookup on
    :type version: int
    :return: List of course data for given dataset version
    :rtype: List[Dict[str, Any]]
    """

    cosmos_client = get_cosmos_client()
    cosmos_db_client = cosmos_client.get_database_client(COSMOS_DATABASE_ID)
    cosmos_container_client = cosmos_db_client.get_container_client(COSMOS_COLLECTION_COURSES)

    query = f"SELECT * from c WHERE c.version = {version}"
    course_list = list(cosmos_container_client.query_items(query, enable_cross_partition_query=True))

    return course_list


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
