from typing import List

from .search_index.index import Index
from .search_index.loader import Loader


def build_index(url: str, api_key: str, api_version: str, index_name: str) -> None:
    try:
        index = Index(url, api_key, api_version, index_name)

        index.delete_if_already_exists()
        index.create()
    except Exception:
        raise


def load_index(url: str, api_key: str, api_version: str, index_name: str, rows: List[str]) -> None:
    try:
        load = Loader(url, api_key, api_version, index_name, rows)

        load.postcode_documents()
    except Exception:
        raise
