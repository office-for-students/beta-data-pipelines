import logging

from services.search_service.base import SearchServiceBase


class SearchServiceLocal(SearchServiceBase):

    def update_index(self):
        logging.info("Updating local search index")

    def update_load(self):
        logging.info("Updating local load index")

    def update_synonyms(self):
        logging.info("Updating local synonyms")
