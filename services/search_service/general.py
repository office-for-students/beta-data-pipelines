from services.search_service.attributes.index import Index
from services.search_service.attributes.load import Load
from services.search_service.attributes.synonym_map import SynonymMap
from services.search_service.base import SearchServiceBase


class SearchService(SearchServiceBase):

    def update_index(self):
        index = Index(self.url, self.api_key, self.api_version, self.version)

        index.delete_if_already_exists()
        index.create()

    def update_load(self):
        load = Load(self.url, self.api_key, self.api_version, self.version, self.docs)

        load.course_documents()

    def update_synonyms(self):
        synonyms = SynonymMap(self.url, self.api_key, self.api_version)

        synonyms.update()
