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
    api_version = os.environ['AzureSearchAPIVersion']

    logging.info(f'config?\nAPI_KEY: {api_key}\nSearch_URL: {search_url}')

    headers = {'Content-Type': 'application/json',
        'api-key': api_key,
        'odata': 'verbose' }

    query_string = '?api-version=' + api_version

    # Get version number
    split_path_name = dataset.name.split('/')
    split_name = split_path_name[1].split('-')

    logging.info(f'what is split_name: {split_name}')
    version = split_name[2]
    index_name = f"courses-{version}"
    
    # Delete search index if it already exists
    delete_url = search_url + "/indexes/" + index_name + query_string
    delete_index_response = requests.delete(delete_url, headers=headers)
    deletion_response = delete_index_response.json()
    logging.info(f"deleting index result: {deletion_response}\nindex-name: {index_name}\n")

    # Create search index
    index_schema = search.get_index_schema(index_name)
    url = search_url + "/indexes" + query_string

    response  = requests.post(url, headers=headers, json=index_schema)
    index = response.json()
    logging.info(f"index result: {index}")

    # Retrieve all courses for a specific version
    courses = utils.get_courses_by_version(version)
    number_of_courses = len(courses)
    logging.info(f"number_of_courses: {number_of_courses}")

    # TODO Add course document to search index
    course_url = search_url + "/indexes/" + index_name + "/docs/index" + query_string
    logging.info(f"what is my url ====================== url: {course_url}")

    course_count = 0
    bulk_course_count = 500
    documents = {}
    search_courses = []
    for course in courses:
        course_count += 1
        logging.info(f"got course: {course_count}")

        search_course = models.build_course_search_doc(course)
        search_courses.append(search_course)

        if course_count % bulk_course_count == 0 or course_count == number_of_courses:
            logging.info(f"about to make request to azure search, number of courses to be uploaded: {course_count}")
            documents['value'] = search_courses
            logging.info(f"check documents before loading, documents: {documents}")
            bulk_response = requests.post(course_url, headers=headers, json=documents)
            logging.info(f"bulk_response: {bulk_response.json()}")

            # Empty variables
            documents = {}
            search_courses = []
        
        