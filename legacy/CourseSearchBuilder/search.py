from typing import Any
from typing import Dict
from typing import List

from legacy.CourseSearchBuilder.search.index import Index
from legacy.CourseSearchBuilder.search.load import Load
from legacy.CourseSearchBuilder.search.synonym_map import SynonymMap


def build_index(url: str, api_key: str, api_version: str, version: int) -> None:
    """
    Creates an object for the search index using the given parameters. If the index already exists, it is replaced
    with a new one.

    :param url: URL for search index
    :type url: str
    :param api_key: API key for search API
    :type api_key: str
    :param api_version: Version of the search API
    :type api_version: str
    :param version: Version of the dataset
    :type version: int
    """
    index = Index(url, api_key, api_version, version)

    index.delete_if_already_exists()
    index.create()


def load_index(url: str, api_key: str, api_version: str, version: int, docs: List[Dict[str, Any]]) -> None:
    """
    Loads courses into the search index at the passed URL using the passed documents list

    :param url: URL of search index
    :type url: str
    :param api_key: API key for search API
    :type api_key: str
    :param api_version: Version of the search API
    :type api_version: str
    :param version: Version of the dataset
    :type version: int
    :param docs: List of course data
    :type docs: List[Dict[str, Any]]
    """
    load = Load(url, api_key, api_version, version, docs)

    load.course_documents()


def build_synonyms(url: str, api_key: str, api_version: str) -> None:
    """
    Builds synonym map for the search index using the passed parameters.

    :param url: URL for search index
    :type url: str
    :param api_key: API key for search API
    :type api_key: str
    :param api_version: Version of the search API
    :type api_version: str
    """
    synonyms = SynonymMap(url, api_key, api_version)

    synonyms.update()
