import logging
from typing import Any
from typing import Dict
from typing import List

import requests

from legacy.PostcodeSearchBuilder import models
from legacy.services import exceptions


class Loader:
    """Loads postcode documents into search index"""

    def __init__(self, url: str, api_key: str, api_version: str, index_name: str, rows: List[str]) -> None:

        self.url = url
        self.headers = {
            'Content-Type': 'application/json',
            'api-key': api_key,
            'odata': 'verbose'
        }
        self.query_string = '?api-version=' + api_version
        self.index_name = index_name

        self.rows = rows

    def postcode_documents(self) -> None:
        """
        Calls method to load courses into search index

        :return: None
        """

        number_of_docs = len(self.rows)
        postcode_count = 0
        bulk_postcode_count = 1000
        documents = {}
        search_postcodes = []
        for row in self.rows:
            postcode_count += 1
            postcode_list = [r.strip() for r in row.split(',')]

            if postcode_count == 1:
                if not models.validate_header(postcode_list):
                    logging.error(f"invalid header row\n\
                                    header_row: {postcode_list}")
                    raise exceptions.StopEtlPipelineErrorException

                logging.info("skipping header row")
                continue

            search_postcode = models.build_postcode_search_doc(postcode_list)
            if search_postcode:
                search_postcodes.append(search_postcode)

            if postcode_count % bulk_postcode_count == 0 or postcode_count == number_of_docs:
                documents['value'] = search_postcodes

                self.bulk_create_postcodes(documents)

                logging.info(
                    f'successfully loaded {postcode_count} postcodes into azure search\n\
                        index: {self.index_name}\n')

                # Empty variables
                documents = {}
                search_postcodes = []

    def bulk_create_postcodes(self, documents: Dict[str, Any]) -> None:
        """
        Sends a post request to the search API to load postcodes into the index.

        :param documents: Postcode data to load into search index
        :type documents: Dict[str, Any]
        """
        try:
            url = self.url + "/indexes/" + self.index_name + "/docs/index" + self.query_string

            response = requests.post(
                url,
                headers=self.headers,
                json=documents
            )

        except requests.exceptions.RequestException as e:
            logging.exception('unexpected error creating index', exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 200:
            logging.error(f'failed to bulk load postcode search documents\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}\n\
                            documents: {documents}')

            raise exceptions.StopEtlPipelineErrorException
