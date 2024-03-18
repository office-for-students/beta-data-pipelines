from typing import Any
from typing import Dict
from typing import List

from legacy.CourseSearchBuilder.search.index import Index
from legacy.CourseSearchBuilder.search.load import Load
from legacy.CourseSearchBuilder.search.synonym_map import SynonymMap


def build_index(url: str, api_key: str, api_version: str, version: int) -> None:
    index = Index(url, api_key, api_version, version)

    index.delete_if_already_exists()
    index.create()


def load_index(url: str, api_key: str, api_version: str, version: int, docs: List[Dict[str, Any]]) -> None:
    load = Load(url, api_key, api_version, version, docs)

    load.course_documents()


def build_synonyms(url: str, api_key: str, api_version: str) -> None:
    synonyms = SynonymMap(url, api_key, api_version)

    synonyms.update()
