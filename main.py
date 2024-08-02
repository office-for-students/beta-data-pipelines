import logging
from datetime import datetime
from logging.config import fileConfig
import os
import json
from fastapi import FastAPI
from starlette.routing import Mount
from starlette.staticfiles import StaticFiles
from constants import LOCAL_COSMOS_CONTAINER_PATH
from constants import COSMOS_COLLECTION_INSTITUTIONS
from constants import BLOB_AZURE_CONNECT_STRING
from constants import BLOB_HESA_BLOB_NAME
from constants import BLOB_HESA_CONTAINER_NAME
from constants import BLOB_SERVICE_MODULE
from constants import COSMOS_CLIENT_MODULE
from constants import COSMOS_COLLECTION_DATASET
from constants import COSMOS_DATABASE_ID
from constants import COSMOS_DATABASE_KEY
from constants import COSMOS_DATABASE_URI
from constants import COSMOS_SERVICE_MODULE
from constants import DATA_SET_SERVICE_MODULE
from constants import DOCS_SPHINX_DIRECTORY
from constants import ENVIRONMENT
from constants import INGESTION_API
from constants import KEY_COSMOS_MASTER_KEY
from constants import SEND_GRID_API_KEY
from constants import SEND_GRID_FROM_EMAIL
from constants import SEND_GRID_TO_EMAILS
from legacy.CourseSearchBuilder.entry import course_search_builder_main
from legacy.CreateDataSet.entry import create_dataset_main
from legacy.CreateInst.entry import create_institutions_main
from legacy.EtlPipeline.entry import etl_pipeline_main
from legacy.PostcodeSearchBuilder.entry import postcode_search_builder_main
from legacy.SubjectBuilder.entry import subject_builder_main
from services import blob_service
from services import cosmos_client
from services import cosmos_service
from services import dataset_service
from services.mail import MailService

# setup loggers
fileConfig('logging.conf', disable_existing_loggers=False)

# get root logger
logger = logging.getLogger(__name__)  # the __name__ resolve to "main" since we are at the root of the project.
# This will get the root logger since no logger in the configuration has this name.


app = FastAPI(
    routes=[
        Mount(
            path="/sphinx",
            app=StaticFiles(directory=DOCS_SPHINX_DIRECTORY, html=True),
            name="sphinx"
        )
    ]
)

MAIL_SERVICE = MailService(
    send_grid_api_key=SEND_GRID_API_KEY,
    from_email=SEND_GRID_FROM_EMAIL,
    to_emails=SEND_GRID_TO_EMAILS,
    enabled=True
)

BLOB_SERVICE = blob_service.get_current_provider(
    provider_path=BLOB_SERVICE_MODULE,
    service_string=BLOB_AZURE_CONNECT_STRING
)

COSMOS_CLIENT = cosmos_client.get_current_provider(
    provider_path=COSMOS_CLIENT_MODULE,
    url=COSMOS_DATABASE_URI,
    credential={KEY_COSMOS_MASTER_KEY: COSMOS_DATABASE_KEY}
)

COSMOS_DATABASE_SERVICE = cosmos_service.get_current_provider(
    provider_path=COSMOS_SERVICE_MODULE,
    cosmos_database=COSMOS_CLIENT.get_database_client(COSMOS_DATABASE_ID)
)

DATASET_SERVICE = dataset_service.get_current_provider(
    provider_path=DATA_SET_SERVICE_MODULE,
    cosmos_container=COSMOS_DATABASE_SERVICE.get_container(
        container_id=COSMOS_COLLECTION_DATASET
    )
)


def send_message(mailer, result, function, error_message=None):
    date_time = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
    message = f"{function} {result}: {date_time}"
    subject = f"{function}: {ENVIRONMENT} - {date_time} - {result} - {error_message if error_message else ''}"
    mailer.send_message(message, subject)


@app.get("/")
async def root():
    return {"message": f"Data Pipeline Version: {INGESTION_API}"}


@app.get("/CourseSearchBuilder/")
async def search_builder():
    logging.info("Search Builder started")
    response = course_search_builder_main(
        blob_service=BLOB_SERVICE,
        cosmos_service=COSMOS_DATABASE_SERVICE,
        dataset_service=DATASET_SERVICE
    )
    return response


@app.get("/CreateDataSet/")
async def create_dataset():
    logging.info("Create Dataset started")
    response = create_dataset_main(
        blob_service=BLOB_SERVICE,
        dataset_service=DATASET_SERVICE,
        storage_container_name=BLOB_HESA_CONTAINER_NAME,  # hesa-raw-xml-ingest
        storage_blob_name=BLOB_HESA_BLOB_NAME,  # latest.xml.gz
    )
    return response


@app.get("/CreateInst/")
async def create_inst():
    logging.info("Create Inst started")
    response = create_institutions_main(
        blob_service=BLOB_SERVICE,
        cosmos_service=COSMOS_DATABASE_SERVICE,
        dataset_service=DATASET_SERVICE
    )
    return response


@app.get("/EtlPipeline/")
async def etl_pipeline():
    logging.info("Etl Pipeline started")
    response = etl_pipeline_main(
        blob_service=BLOB_SERVICE,
        dataset_service=DATASET_SERVICE,
        cosmos_service=COSMOS_DATABASE_SERVICE
    )
    return response


@app.get("/PostcodeSearchBuilder/")
async def postcode_search_builder():
    logging.info("Postcode Search Builder started")
    response = postcode_search_builder_main(
        blob_service=BLOB_SERVICE,
    )
    return response


@app.get("/SubjectBuilder/")
async def subject_builder():
    logging.info("Subject Builder started")
    response = subject_builder_main(
        blob_service=BLOB_SERVICE,
        cosmos_service=COSMOS_DATABASE_SERVICE,
        dataset_service=DATASET_SERVICE
    )
    return response


@app.get("/countInst/")
async def subject_builder():
    '''
    Get list of all institutions and how many institutions - debugging purposes
    '''

    file_path = os.getcwd() + LOCAL_COSMOS_CONTAINER_PATH + COSMOS_COLLECTION_INSTITUTIONS + ".json"
    print(file_path)
    with open(file_path, 'r') as file:
        data = json.load(file)['5']
        all_institutions = []
        not_duplicated = []
        for inst in data:
            all_institutions.append(inst['institution']['legal_name']) # get every one
            if inst['institution']['legal_name'] not in not_duplicated:
                not_duplicated.append(inst['institution']['legal_name']) # only get it if its not a duplicate
        len_all_institutions = len(all_institutions)
        len_not_duplicate = len(not_duplicated)
        sorted_all_institutions = sorted(all_institutions)
        sorted_not_duplicate = sorted(not_duplicated)

    data = {
        "len_all_institutions": len_all_institutions,
        "number_of_duplicates": len_all_institutions - len_not_duplicate,
        "len_not_duplicate": len_not_duplicate,
        "not_duplicated": sorted_not_duplicate,

    }
    return data