import logging
from datetime import datetime

from decouple import config
from fastapi import FastAPI

from legacy.CourseSearchBuilder.entry import course_search_builder_main
from legacy.CreateDataSet.entry import create_dataset_main
from legacy.CreateInst.entry import create_institutions_main
from legacy.EtlPipeline.entry import etl_pipeline_main
from legacy.PostcodeSearchBuilder.entry import postcode_search_builder_main
from legacy.SubjectBuilder.entry import subject_builder_main
from legacy.services.blob import BlobService
from legacy.services.exceptions import DataSetTooEarlyError
from legacy.services.exceptions import StopEtlPipelineErrorException
from legacy.services.mail import MailService

app = FastAPI()
mail_helper = MailService(
    send_grid_api_key=config("SendGridAPIKey"),
    from_email=config("SendGridFromEmail"),
    to_emails=config("SendGridToEmailList"),
    enabled=True
)
dataset_service = DataSetService(

)

BLOB_ACCOUNT_NAME = config("BLOB_ACCOUNT_NAME")
BLOB_ACCOUNT_KEY = config("BLOB_ACCOUNT_KEY")
BLOB_AZURE_CONNECT_STRING = f"DefaultEndpointsProtocol=https;AccountName={BLOB_ACCOUNT_NAME};AccountKey={BLOB_ACCOUNT_KEY};EndpointSuffix=core.windows.net"

BLOB_SERVICE = BlobService(
    azure_storage_connection_string=BLOB_AZURE_CONNECT_STRING
)

HESA_STORAGE_CONTAINER_NAME = config("BLOB_HESA_CONTAINER_NAME")
HESA_STORAGE_BLOB_NAME = config("BLOB_HESA_BLOB_NAME")

# Local test xml file is to be used for testing the pipeline locally
# defaults to None, as in production this will not be set
LOCAL_TEST_XML_FILE = config("XML_LOCAL_TEST_XML_FILE", default=None)

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
    # send_message(mail_helper, "Started", "Search Builder")
    logging.info("Search Builder started")
    error = None
    try:
        course_search_builder_main()
    except Exception as e:
        message = f"Search Builder failed: {e}"
        error = e
    else:
        message = f"Search Builder succeeded"

    logging.info(message)
    # send_message(mail_helper, "Succeeded" if not error else "Failed", "Search Builder", error_message=error)
    return {"message": message}


@app.get("CreateDataSet/")
async def create_dataset():
    # send_message(mail_helper, "Started", "Create Dataset")
    logging.info("Create Dataset started")
    error = None
    try:
        create_dataset_main(
            blob_service=BLOB_SERVICE,
            storage_container_name=HESA_STORAGE_CONTAINER_NAME,  # hesa-raw-xml-ingest
            storage_blob_name=HESA_STORAGE_BLOB_NAME,  # latest.xml.gz
        )
    except DataSetTooEarlyError as e:
        message = f"Create Dataset failed: {e}"
        error = e
    except StopEtlPipelineErrorException as e:
        message = f"Create Dataset failed: {e}"
        error = e
    except Exception as e:
        message = f"Create Dataset failed: {e}"
        error = e
    else:
        message = "Create Dataset succeeded"

    # send_message(mail_helper, "Succeeded" if not error else "Failed", "Create Dataset", error_message=error)
    logging.info(message)
    return {"message": message}


@app.get("CreateInst/")
async def create_inst():
    logging.info("Create Institutions started")
    # send_message(mail_helper, "Started", "Create Institutions")
    error = None
    try:
        create_institutions_main()
    except Exception as e:
        message = f"Create Institutions failed: {e}"
        error = e
    else:
        message = "Create Institutions succeeded"

    logging.info(message)
    # send_message(mail_helper, "Succeeded" if not error else "Failed", "Create Institutions", error_message=error)
    return {"message": message}


@app.get("EtlPipeline/")
async def etl_pipeline():
    # send_message(mail_helper, "Started", "Etl Pipeline")
    logging.info("Etl Pipeline started")
    error = None
    try:
        etl_pipeline_main(
            blob_service=BLOB_SERVICE,
            hesa_container_name=HESA_STORAGE_CONTAINER_NAME,
            hesa_blob_name=HESA_STORAGE_BLOB_NAME
        )
    except Exception as e:
        message = f"Etl Pipeline failed: {e}"
        error = e
    else:
        message = f"Etl Pipeline succeeded"

    logging.info(message)
    # send_message(mail_helper, "Succeeded" if not error else "Failed", "Etl Pipeline", error_message=error)
    return {"message": message}


@app.get("PostcodeSearchBuilder/")
async def postcode_search_builder():
    # send_message(mail_helper, "Started", "Postcode Search Builder")
    logging.info("Postcode Search Builder started")
    error = None
    try:
        postcode_search_builder_main()
    except Exception as e:
        message = f"Postcode Search Builder failed: {e}"
        error = e
    else:
        message = f"Postcode Search Builder succeeded"
    # send_message(mail_helper, "Succeeded" if not error else "Failed", "Postcode Search Builder", error_message=error)
    return {"message": message}


@app.get("SubjectBuilder/")
async def subject_builder(name: str):
    # send_message(mail_helper, "Started", "Subject Builder")
    logging.info("Subject Builder started")
    error = None
    try:
        subject_builder_main()
    except Exception as e:
        message = f"Subject Builder failed: {e}"
        error = e
    else:
        message = f"Subject Builder succeeded"
    # send_message(mail_helper, "Succeeded" if not error else "Failed", "Subject Builder", error_message=error)
    return {"message": message}
