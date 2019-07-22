import logging
import requests
import json
import os

from SharedCode import exceptions
from . import models


def build_index(url, api_key, api_version, version):

    try:
        index = Index(url, api_key, api_version, version)

        index.delete_if_already_exists()
        index.create()
    except Exception:
        raise


def load_index(url, api_key, api_version, version, docs):

    try:
        load = Load(url, api_key, api_version, version, docs)

        load.course_documents()
    except Exception:
        raise


class Index():
    """Creates a new index"""
    def __init__(self, url, api_key, api_version, version):

        self.query_string = '?api-version=' + api_version
        self.index_name = f"courses-{version}"
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

        if response.status_code == 204:
            logging.warn(
                f'course search index already exists, successful deletion\
                           prior to recreation\n index: {self.index_name}')

        elif response.status_code != 404:
            # 404 is the expected response, because normally the
            # course search index will not exist
            logging.error(
                f'unexpected response when deleting existing search index,\
                            search_response: {response.json()}\nindex-name:\
                            {self.index_name}\nstatus: {response.status_code}')

            raise exceptions.StopEtlPipelineErrorException

    def create(self):
        self.get_index()

        try:
            create_url = self.url + "/indexes" + self.query_string
            response = requests.post(create_url,
                                     headers=self.headers,
                                     json=self.course_schema)

        except requests.exceptions.RequestException as e:
            logging.exception('unexpected error creating index', exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 201:
            logging.error(f'failed to create search index\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}')

            raise exceptions.StopEtlPipelineErrorException

    def get_index(self):
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, 'schemas/course.json')) as json_file:
            schema = json.load(json_file)
            schema['name'] = self.index_name

            self.course_schema = schema


class Load():
    """Loads course documents into search index"""
    def __init__(self, url, api_key, api_version, version, docs):

        self.url = url
        self.headers = {
            'Content-Type': 'application/json',
            'api-key': api_key,
            'odata': 'verbose'
        }
        self.query_string = '?api-version=' + api_version
        self.index_name = f"courses-{version}"

        self.docs = docs

    def course_documents(self):

        number_of_docs = len(self.docs)
        course_count = 0
        bulk_course_count = 500
        documents = {}
        search_courses = []
        for doc in self.docs:
            course_count += 1

            search_course = models.build_course_search_doc(doc)
            search_courses.append(search_course)

            if course_count % bulk_course_count == 0 or\
               course_count == number_of_docs:

                documents['value'] = search_courses

                self.bulk_create_courses(documents)

                logging.info(
                    f'successfully loaded {course_count} courses into azure search\n\
                        index: {self.index_name}\n')

                # Empty variables
                documents = {}
                search_courses = []

    def bulk_create_courses(self, documents):

        try:
            url = self.url + "/indexes/" + self.index_name + \
                "/docs/index" + self.query_string
            response = requests.post(url, headers=self.headers, json=documents)
        except requests.exceptions.RequestException as e:
            logging.exception('unexpected error creating index', exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 200:
            logging.error(f'failed to bulk load course search documents\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}')

            raise exceptions.StopEtlPipelineErrorException
