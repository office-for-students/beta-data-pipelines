import os
import logging
import traceback

import azure.functions as func
from SharedCode import utils
from . import search, helpers


def main(dataset: func.InputStream):

    logging.info(
        f"Python blob trigger function processed blob \n"
        f"Name: {dataset.name}\n"
        f"Blob Size: {dataset.length} bytes\n"
    )

    api_key = os.environ["SearchAPIKey"]
    search_url = os.environ["SearchURL"]
    api_version = os.environ["AzureSearchAPIVersion"]

    try:

        # Get version number
        version = helpers.get_version(dataset.name)

        # Upsert (create or update) synonyms
        search.build_synonyms(search_url, api_key, api_version)

        # Create search index
        search.build_index(search_url, api_key, api_version, version)

        # Retrieve all courses for a specific version
        courses = utils.get_courses_by_version(version)

        number_of_courses = len(courses)

        # Add course documents to search index
        logging.info(
            f"attempting to load courses to azure search\n\
                        number_of_courses: {number_of_courses}\n"
        )

        search.load_index(search_url, api_key, api_version, version, courses)

        logging.info(f"Successfully loaded search documents\n")

    except Exception as e:
        # Unexpected exception
        logging.error("Unexpected extension")
        logging.error(traceback.format_exc())

        # Raise to Azure
        raise e
