import json
import logging
import os

import requests

from legacy.services import exceptions


class SynonymMap:
    """Creates a new synonym"""

    # TODO review this code and tidy up where needed when time permits

    def __init__(self, url, api_key, api_version) -> None:

        self.query_string = "?api-version=" + api_version
        self.synonym_name = "english-course-title"
        self.url = url

        self.headers = {"Content-Type": "application/json", "api-key": api_key}

    def update(self) -> None:
        """
        Sends a put request to the API to update the synonym map
        """
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

    def create(self) -> None:
        """
        Sends a put request to the API to create a synonym map
        """
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

    def get_synonym(self) -> None:
        """
        Sets the synonym map schema name using the synonym map json
        """
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, "schemas/course_synonym.json")) as json_file:
            schema = json.load(json_file)
            schema["name"] = self.synonym_name
            schema["synonyms"] = self.get_synonym_list()

            self.course_synonym_schema = schema

    @staticmethod
    def get_synonym_list() -> str:
        """
        Returns a hardcoded list of synonyms for subject search

        :return: String of synonyms for subject search
        :rtype: str
        """
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
