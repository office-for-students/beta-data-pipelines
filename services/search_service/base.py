from typing import Any
from typing import Dict
from typing import List


class SearchServiceBase(object):
    def __init__(self, url: str, api_key: str, api_version: str, version: int, docs: List[Dict[str, Any]]):
        """


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
        :return: Self
        """
        self.url = url
        self.api_key = api_key
        self.api_version = api_version
        self.version = version
        self.docs = docs

    def update_microsoft_search(self):
        self.update_index()
        self.update_load()
        self.update_synonyms()

    def update_index(self):
        pass

    def update_load(self):
        pass

    def update_synonyms(self):
        pass
