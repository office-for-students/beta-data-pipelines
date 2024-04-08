import logging
from typing import Any
from typing import Dict
from typing import List

import requests

from legacy.CourseSearchBuilder import models
from services import exceptions


class Load:
    """Loads course documents into search index"""

    def __init__(self, url: str, api_key: str, api_version: str, version: int, docs: List) -> None:

        self.url = url
        self.headers = {
            "Content-Type": "application/json",
            "api-key": api_key,
            "odata": "verbose",
        }
        self.query_string = "?api-version=" + api_version
        self.index_name = f"courses-{version}"

        self.docs = docs

    def course_documents(self) -> None:
        """
        Calls method to load courses into search index
        """

        number_of_docs = len(self.docs)
        logging.info(f"THERE ARE A TOTAL OF {number_of_docs} courses")
        course_count = 0
        bulk_course_count = 500
        documents = {}
        search_courses = []
        for doc in self.docs:
            course_count += 1

            search_course = models.build_course_search_doc(doc)
            search_courses.append(search_course)

            if course_count % bulk_course_count == 0 or course_count == number_of_docs:
                documents["value"] = search_courses

                self.bulk_create_courses(documents)

                logging.info(
                    f"successfully loaded {course_count} courses into azure search\n\
                        index: {self.index_name}\n"
                )

                # Empty variables
                documents = {}
                search_courses = []

    def bulk_create_courses(self, documents: Dict[str, Any]) -> None:
        """
        Sends a post request to the search API to load courses into the index.

        :param documents: Course data to load into search index
        :type documents: Dict[str, Any]
        """

        try:
            url = (
                    self.url
                    + "/indexes/"
                    + self.index_name
                    + "/docs/index"
                    + self.query_string
            )
            logging.info(f"url: {url}")
            response = requests.post(url, headers=self.headers, json=documents)
        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error loading bulk index", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 200:
            logging.error(
                f"failed to bulk load course search documents\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException()
