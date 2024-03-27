from typing import List

from .search_index.index import Index
from .search_index.loader import Loader


def build_index(url: str, api_key: str, api_version: str, index_name: str) -> None:
    """
    Creates an object for the search index using the given parameters. If the index already exists, it is replaced
    with a new one.

    :param url: URL for search index
    :type url: str
    :param api_key: API key for search API
    :type api_key: str
    :param api_version: Version of the search API
    :type api_version: str
    :param index_name: Name of the created index
    :type index_name: str
    :return: None
    """
    try:
        index = Index(
            url=url,
            api_key=api_key,
            api_version=api_version,
            index_name=index_name
        )

        index.delete_if_already_exists()
        index.create()
    except Exception:
        raise


def load_index(url: str, api_key: str, api_version: str, index_name: str, rows: List[str]) -> None:
    """
    Loads postcodes into the search index at the passed URL

    :param url: URL of search index
    :type url: str
    :param api_key: API key for search API
    :type api_key: str
    :param api_version: Version of the search API
    :type api_version: str
    :param index_name: Name of the search index
    :type index_name: str
    :param rows: List of rows containing postcode data
    :type rows: List[str]
    :return: None
    """
    try:
        load = Loader(
            url=url,
            api_key=api_key,
            api_version=api_version,
            index_name=index_name,
            rows=rows)

        load.postcode_documents()
    except Exception:
        raise
