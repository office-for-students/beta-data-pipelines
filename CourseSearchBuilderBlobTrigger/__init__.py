import os
import logging
import requests

from datetime import datetime

import azure.functions as func
from SharedCode import utils
from . import search, models

def main(dataset: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {dataset.name}\n"
                 f"Blob Size: {dataset.length} bytes")

    api_key = os.environ['SearchAPIKey']
    search_url = os.environ['SearchURL']

    logging.info(f'config?\nAPI_KEY: {api_key}\nSearch_URL: {search_url}')

    headers = {'Content-Type': 'application/json',
        'api-key': api_key }

    api_version = '?api-version=2019-05-06'

    # Get version number
    split_path_name = dataset.name.split('/')
    split_name = split_path_name[1].split('-')

    logging.info(f'what is split_name: {split_name}')
    version = split_name[2]
    index_name = f'courses-{version}'

    index_schema = search.get_index_schema(index_name)

    # Create search index
    url = search_url + "/indexes" + api_version
    response  = requests.post(url, headers=headers, json=index_schema)
    index = response.json()
    logging.info(f"index result: {index}")

    # Retrieve all courses for a specific version
    courses = utils.get_courses_by_version(version)
    number_of_courses = len(courses)
    logging.info(f"number_of_courses: {number_of_courses}")

    # TODO Add course document to search index
    course_url = search_url + "/indexes/" + index_name + "/docs" + api_version

    course_count = 0
    bulk_course_count = 500
    outer_wrapper = {}
    search_courses = []
    for course in courses:
        course_count += 1

        search_course = models.build_course_search_doc(course)
        search_courses.append(search_course)

        if course_count % bulk_course_count == 0 or course_count == number_of_courses:
            outer_wrapper['value'] = courses
            response  = requests.post(course_url, headers=headers, json=outer_wrapper)

            # Empty variables
            outer_wrapper = {}
            search_courses = []
        
        