import logging
import requests
import json
import os

from . import exceptions
from . import models


def build_index(url, api_key, api_version, index_name):

    try:
        index = Index(url, api_key, api_version, index_name)

        index.delete_if_already_exists()
        index.create()
    except Exception:
        raise


def load_index(url, api_key, api_version, index_name, rows):

    try:
        load = Loader(url, api_key, api_version, index_name, rows)

        load.postcode_documents()
    except Exception:
        raise


class Index():
    """Creates a new index"""
    def __init__(self, url, api_key, api_version, index_name):

        self.query_string = '?api-version=' + api_version
        self.index_name = index_name
        self.url = url

        self.headers = {
            'Content-Type': 'application/json',
            'api-key': api_key,
            'odata': 'verbose'
        }

    def delete_if_already_exists(self):

        try:
            delete_url = self.url + "/indexes/" + self.index_name + \
                self.query_string

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

    def create(self):
        self.build_postcode_schema()

        try:
            create_url = self.url + "/indexes" + self.query_string
            response = requests.post(create_url,
                                     headers=self.headers,
                                     json=self.postcode_schema)

        except requests.exceptions.RequestException as e:
            logging.exception('unexpected error creating index', exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 201:
            logging.error(f'failed to create search index\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}')

            raise exceptions.StopEtlPipelineErrorException

    def build_postcode_schema(self):
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, 'schemas/postcode.json')) as json_file:
            schema = json.load(json_file)
            schema['name'] = self.index_name

            self.postcode_schema = schema


class Loader():
    """Loads postcode documents into search index"""
    def __init__(self, url, api_key, api_version, index_name, rows):

        self.url = url
        self.headers = {
            'Content-Type': 'application/json',
            'api-key': api_key,
            'odata': 'verbose'
        }
        self.query_string = '?api-version=' + api_version
        self.index_name = index_name

        self.rows = rows

    def postcode_documents(self):

        number_of_docs = len(self.rows)
        postcode_count = 0
        bulk_postcode_count = 1000
        documents = {}
        search_postcodes = []
        for row in self.rows:
            postcode_count += 1
            postcode_list = [r.strip() for r in row.split(',')]

            if postcode_count == 1:
                if models.validate_header(postcode_list):
                    logging.error(f"invalid header row\n\
                                    header_row: {postcode_list}")
                    raise exceptions.StopEtlPipelineErrorException
                    break

                logging.info("skipping header row")
                continue

            search_postcode = models.build_postcode_search_doc(postcode_list)
            if search_postcode:
                search_postcodes.append(search_postcode)

            if postcode_count % bulk_postcode_count == 0 or\
               postcode_count == number_of_docs:

                documents['value'] = search_postcodes

                self.bulk_create_postcodes(documents)

                logging.info(
                    f'successfully loaded {postcode_count} postcodes into azure search\n\
                        index: {self.index_name}\n')

                # Empty variables
                documents = {}
                search_postcodes = []

    def bulk_create_postcodes(self, documents):
        try:
            url = self.url + "/indexes/" + self.index_name + \
                "/docs/index" + self.query_string

            response = requests.post(url, headers=self.headers, json=documents)

        except requests.exceptions.RequestException as e:
            logging.exception('unexpected error creating index', exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 200:
            logging.error(f'failed to bulk load postcode search documents\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}\n\
                            documents: {documents}')

            raise exceptions.StopEtlPipelineErrorException
