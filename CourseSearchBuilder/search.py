import logging
import requests
import json
import os

from __app__.SharedCode import exceptions
from . import models


def build_index(url, api_key, api_version, version):
    index = Index(url, api_key, api_version, version)

    index.delete_if_already_exists()
    index.create()


def load_index(url, api_key, api_version, version, docs):
    load = Load(url, api_key, api_version, version, docs)

    load.course_documents()


def build_synonyms(url, api_key, api_version):
    synonyms = SynonymMap(url, api_key, api_version)

    synonyms.update()


class Index:
    """Creates a new index"""

    def __init__(self, url, api_key, api_version, version):

        self.query_string = "?api-version=" + api_version
        self.index_name = f"courses-{version}"
        self.url = url

        self.headers = {
            "Content-Type": "application/json",
            "api-key": api_key,
            "odata": "verbose",
        }

    def delete_if_already_exists(self):

        try:
            delete_url = self.url + "/indexes/" + self.index_name + self.query_string

            response = requests.delete(delete_url, headers=self.headers)

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error deleting index", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code == 204:
            logging.warn(
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

    def create(self):
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

    def get_index(self):
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, "schemas/course.json")) as json_file:
            schema = json.load(json_file)
            schema["name"] = self.index_name

            self.course_schema = schema


class Load:
    """Loads course documents into search index"""

    def __init__(self, url, api_key, api_version, version, docs):

        self.url = url
        self.headers = {
            "Content-Type": "application/json",
            "api-key": api_key,
            "odata": "verbose",
        }
        self.query_string = "?api-version=" + api_version
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

    def bulk_create_courses(self, documents):

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


class SynonymMap:
    """Creates a new synonym"""

    # TODO review this code and tidy up where needed when time permits

    def __init__(self, url, api_key, api_version):

        self.query_string = "?api-version=" + api_version
        self.synonym_name = "english-course-title"
        self.url = url

        self.headers = {"Content-Type": "application/json", "api-key": api_key}

    def update(self):
        self.get_synonym()

        try:
            update_url = (
                self.url + "/synonymmaps/" + self.synonym_name + self.query_string
            )
            response = requests.put(
                update_url, headers=self.headers, json=self.course_synonym_schema
            )

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error creating course synonym", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code == 201:
            # PUT can return 201 if a new resource is created
            return

        if response.status_code == 204:
            return

        if response.status_code == 404:
            logging.warning(
                f"failed to update course search synonyms, unable to find synonym; try creating synonym\n\
                            synonym-name: {self.synonym_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            self.create()
        else:
            logging.error(
                f"failed to update course search synonyms\n\
                            synonym-name: {self.synonym_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def create(self):
        try:
            create_url = self.url + "/synonymmaps" + self.query_string
            response = requests.put(
                create_url, headers=self.headers, json=self.course_synonym_schema
            )

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error creating course synonym", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 201:
            logging.error(
                f"failed to create course search synonyms, after failing to update synonyms\n\
                            synonym-name: {self.synonym_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def get_synonym(self):
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, "schemas/course_synonym.json")) as json_file:
            schema = json.load(json_file)
            schema["name"] = self.synonym_name
            schema["synonyms"] = self.get_synonym_list()

            self.course_synonym_schema = schema

    def get_synonym_list(self):
        dentist_synonyms = "dentist, dentistry, endodontics, orthodontics, dentofacial orthopedics, oral medicine, pediatric dentistry, public health dentistry, prosthodontist, tooth doctor, dental surgeon, dental practitioner, hygenist, clinical dentistry, preclinical dentistry, pre clinical dentistry, pre-clinical dentistry => dentistry"
        midwidfery_synonyms = "obstetrics, obstetrical delivery, perinatology, fetology, feotology, tocology => midwifery"
        dental_nursing_synonyms = (
            "dentistry, hygenist, dental practitioner => dental nursing"
        )
        mental_health_nursing_synonyms = "registered mental health nursing(RMN), mental health nurse, dual diagnosis, mental health => mental health nursing"
        pharmacology_synonyms = "pharmacology, psychopharmacology, pharmacological medicine, pharmacokenetics, pharmacodynamics, medical speciality, toxicology, substances, drugs => pharmacology"
        toxicology_synonyms = "pharmacology, pharmacological medicine => toxicology"
        pharmacy_synonyms = "pharmacy, pharmacist, chemist => pharmacy"
        chemistry_synonyms = "chemical science, inorganic chemistry, surface chemistry, geochemistry, natural science, radiochemistry, thermochemistry, organic chemistry, physical chemistry, photochemistry, chemoimmunology, immunochemistry, femtochemistry => chemistry"
        mathematics_synonyms = "stat, stats, statistic, statistics, maths, math, applied mathematics, pure maths, pure math, pure mathematics, applied maths, applied math => mathematics"
        civil_engineering_synonyms = "construction, roads, bridges, applied science, engineering science, engineering, hydrolic engineering => civil engineering"

        synonyms = {
            dentist_synonyms,
            midwidfery_synonyms,
            dental_nursing_synonyms,
            mental_health_nursing_synonyms,
            pharmacology_synonyms,
            toxicology_synonyms,
            pharmacy_synonyms,
            chemistry_synonyms,
            mathematics_synonyms,
            civil_engineering_synonyms,
        }

        return "\n".join(synonyms)
