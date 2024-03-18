import json
import logging
import os

import requests

from legacy.services import exceptions


class Index:
    """Creates a new index"""

    def __init__(self, url: str, api_key: str, api_version: str, version: int) -> None:

        self.query_string = "?api-version=" + api_version
        self.index_name = f"courses-{version}"
        self.url = url

        self.headers = {
            "Content-Type": "application/json",
            "api-key": api_key,
            "odata": "verbose",
        }

    def delete_if_already_exists(self) -> None:

        try:
            delete_url = self.url + "/indexes/" + self.index_name + self.query_string

            response = requests.delete(delete_url, headers=self.headers)

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error deleting index", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code == 204:
            logging.warning(
                f"course search index already exists, successful deletion\
                           prior to recreation\n index: {self.index_name}"
            )

        elif response.status_code != 404:
            # 404 is the expected response, because normally the
            # course search index will not exist
            logging.error(
                f"unexpected response when deleting existing search index,\
                            search_response: {response.json()}\nindex-name:\
                            {self.index_name}\nstatus: {response.status_code}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def create(self) -> None:
        self.get_index()

        try:
            create_url = self.url + "/indexes" + self.query_string
            response = requests.post(
                create_url, headers=self.headers, json=self.course_schema
            )

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error creating index", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 201:
            logging.error(
                f"failed to create search index\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def get_index(self) -> None:
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, "schemas/course.json")) as json_file:
            schema = json.load(json_file)
            schema["name"] = self.index_name

            self.course_schema = schema
