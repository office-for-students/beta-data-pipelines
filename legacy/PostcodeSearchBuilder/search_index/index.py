import json
import logging
import os

import requests

from services import exceptions


class Index:
    """Creates a new index"""

    def __init__(self, url: str, api_key: str, api_version: str, index_name: str) -> None:

        self.query_string = '?api-version=' + api_version
        self.index_name = index_name
        self.url = url
        self.postcode_schema = None

        self.headers = {
            'Content-Type': 'application/json',
            'api-key': api_key,
            'odata': 'verbose'
        }

    def delete_if_already_exists(self) -> None:
        """
        Sends delete request to search index URL if it already exists.

        :return: None
        """

        try:
            delete_url = self.url + "/indexes/" + self.index_name + self.query_string

            response = requests.delete(delete_url, headers=self.headers)

        except requests.exceptions.RequestException as e:
            logging.exception('unexpected error deleting index', exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code == 404:
            logging.warning(
                f'postcode search index not found, unable to delete index\
                           prior to recreation\n index: {self.index_name}')

        elif response.status_code != 204:
            # 204 is the expected response, because normally there
            # will always be a postcode search index that already exists
            logging.error(
                f'unexpected response when deleting existing search index,\
                            search_response: {response.json()}\nindex-name:\
                            {self.index_name}\nstatus: {response.status_code}')

            raise exceptions.StopEtlPipelineErrorException

    def create(self) -> None:
        """
        Sends post request to the search index URL.

        :return: None
        """
        self.build_postcode_schema()

        try:
            create_url = self.url + "/indexes" + self.query_string
            response = requests.post(
                create_url,
                headers=self.headers,
                json=self.postcode_schema
            )

        except requests.exceptions.RequestException as e:
            logging.exception('unexpected error creating index', exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 201:
            logging.error(f'failed to create search index\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}')

            raise exceptions.StopEtlPipelineErrorException

    def build_postcode_schema(self) -> None:
        """
        Sets the postcode schema name using the postcode JSON

        :return: None
        """
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, 'schemas/postcode.json')) as json_file:
            schema = json.load(json_file)
            schema['name'] = self.index_name

            self.postcode_schema = schema
