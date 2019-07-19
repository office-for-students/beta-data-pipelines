import os
import logging
import traceback

import azure.functions as func
from SharedCode import utils
from . import search, helpers


def main(dataset: func.InputStream):

    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {dataset.name}\n"
                 f"Blob Size: {dataset.length} bytes\n")

    api_key = os.environ['SearchAPIKey']
    search_url = os.environ['SearchURL']
    api_version = os.environ['AzureSearchAPIVersion']

    try:
        headers = {'Content-Type': 'application/json',
                   'api-key': api_key,
                   'odata': 'verbose'}

        query_string = '?api-version=' + api_version

        # Get version number
        version = helpers.get_version(dataset.name)

        index_name = f"courses-{version}"

        # Delete search index if it already exists
        delete_url = search_url + "/indexes/" + index_name + query_string

        search.delete_index_if_exists(delete_url, headers, index_name)

        # Create search index
        index_schema = search.get_index_schema(index_name)
        url = search_url + "/indexes" + query_string
        search.create_index(url, headers, index_schema, index_name)

        # Retrieve all courses for a specific version
        courses = utils.get_courses_by_version(version)

        number_of_courses = len(courses)

        # Add course documents to search index
        course_url = search_url + "/indexes/" + index_name + "/docs/index" + \
            query_string

        logging.info(f'attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n')

        search.load_course_documents(course_url, headers, index_name, courses)

        logging.info(f'Successfully loaded search documents\n')

    except Exception as e:
        # Unexpected exception
        logging.error('Unexpected extension')
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
