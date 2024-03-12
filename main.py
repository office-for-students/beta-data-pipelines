import logging
from datetime import datetime

from decouple import config
from fastapi import FastAPI

from legacy.CourseSearchBuilder.entry import build_search
from legacy.CreateDataSet.entry import create_dataset as create_dataset_legacy
from legacy.services.blob_helper import BlobHelper

from legacy.CreateInst.entry import create_institutions
from legacy.EtlPipeline.entry import create_courses
from legacy.services.exceptions import DataSetTooEarlyError
from legacy.services.exceptions import StopEtlPipelineErrorException
from mail_helper import MailHelper

app = FastAPI()
mail_helper = MailHelper(
    send_grid_api_key=config("SendGridAPIKey"),
    to_emails=config("SendGridToEmailList"),
    from_email=config("SendGridFromEmail"),
    enabled=True
)

BLOB_ACCOUNT_NAME = config("BLOB_ACCOUNT_NAME")
BLOB_ACCOUNT_KEY = config("BLOB_ACCOUNT_KEY")
BLOB_AZURE_CONNECT_STRING = f"DefaultEndpointsProtocol=https;AccountName={BLOB_ACCOUNT_NAME};AccountKey={BLOB_ACCOUNT_KEY};EndpointSuffix=core.windows.net"

BLOB_HELPER = BlobHelper(
    azure_storage_connection_string=BLOB_AZURE_CONNECT_STRING
)

HESA_STORAGE_CONTAINER_NAME = config("BLOB_HESA_CONTAINER_NAME")
HESA_STORAGE_BLOB_NAME = config("BLOB_HESA_BLOB_NAME")

# Local test xml file is to be used for testing the pipeline locally
# defaults to None, as in production this will not be set
LOCAL_TEST_XML_FILE = config("LOCAL_TEST_XML_FILE", default=None)

ENVIRONMENT = config("Environment")  # i.e. dev, pre-prod or prod
STORAGE_URL = config("StorageUrl")


def send_message(mailer, result, function, error_message=None):
    date_time = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
    message = f"{function} {result}: {date_time}"
    subject = f"{function}: {ENVIRONMENT} - {date_time} - {result} - {error_message if error_message else ''}"
    mailer.send_message(message, subject)


@app.get("/")
async def root():
    return {"message": f"Data Pipeline Version: {config('INGESTION_API')}"}


@app.get("CourseSearchBuilder/")
async def search_builder():
    send_message(mail_helper, "Started", "Search Builder")
    logging.info("Search Builder started")
    try:
        response = build_search()
        logging.info("Search Builder succeeded")
        send_message(mail_helper, "Succeeded", "Search Builder")
        return response
    except Exception as e:
        logging.error(f"Search Builder failed {e}")
        send_message(mail_helper, "Failed", "Search Builder", error_message=e)


@app.get("CreateDataSet/")
async def create_dataset():
    logging.info("Create Dataset started")
    send_message(mail_helper, "Started", "Create Dataset")
    try:
        create_dataset_legacy(
            blob_service=
            BLOB_HELPER,
            HESA_STORAGE_CONTAINER_NAME, # hesa-raw-xml-ingest
            HESA_STORAGE_BLOB_NAME, # latest.xml.gz
        )
        send_message(mail_helper, "Succeeded", "Create Dataset")
        logging.info("Create Dataset succeeded")
        return {"message": f"created dataset"}
    except DataSetTooEarlyError:
        logging.info(f"Create Dataset failed {e}")
        send_message(mail_helper, "Failed", "Create Dataset", error_message=e)
    except StopEtlPipelineErrorException as e:
        logging.info(f"Create Dataset failed {e}")
        send_message(mail_helper, "Failed", "Create Dataset", error_message=e)
    except Exception as e:
        logging.info(f"Create Dataset failed {e}")
        send_message(mail_helper, "Failed", "Create Dataset", error_message=e)



@app.get("CreateInst/")
async def create_inst():
    logging.info("Create Institutions started")
    send_message(mail_helper, "Started", "Create Institutions")
    try:
        create_institutions()
    except Exception as e:
        logging.info(f"Create Institutions failed {e}")
        send_message(mail_helper, "Failed", "Create Institutions", error_message=e)

    return {"message": f"create institution"}


@app.get("EtlPipeline/")
async def etl_pipeline():
    logging.info("Etl Pipeline started")

    create_courses(
        blob_service=BLOB_HELPER,
        hesa_container_name=HESA_STORAGE_CONTAINER_NAME,
        hesa_blob_name=HESA_STORAGE_BLOB_NAME
    )

    return {"message": f"etl pipeline"}


@app.get("PostcodeSearchBuilder/")
async def postcode_search_builder():
    return {"message": f"post code search builder"}


@app.get("SubjectBuilder/")
async def subject_builder(name: str):
    return {"message": f"subject_builder"}
