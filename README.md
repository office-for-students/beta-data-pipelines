Documentation
=============

Docs are created using the built in functionality of FastAPI and can be accessed via:

http://127.0.0.1:8000/docs


OfS Serverless Data Pipelines
=============================
OfS Serverless Data Ingestion and ETL Pipelines using Azure Functions and Azure Cosmos DB

About the Project
=================

Project leverages Azure Functions to create a serverless data ingestion and ETL pipeline.

The purpose of the project is to create a pipeline that can be used to ingest data from
[HESA data service](https://www.hesa.ac.uk/data-and-analysis/students).
These data pipelines transform the data into a common format and store the data in a datastore that can be used to
power a search engine.

The project is currently being used to power the [Discover Uni](https://discoveruni.gov.uk/) website.

Getting Started
===============

Code is written in Python 3.11+ and uses:

- [Sphinx](https://www.sphinx-doc.org/en/master/): Automatic generation of documentation
- [myst-parser](https://myst-parser.readthedocs.io/en/latest/): Parser to include .md files into .rst files for Sphinx documentation
- [sphinx-rtd-theme](https://pypi.org/project/sphinx-rtd-theme/): Theme for Sphinx documentation
- [Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/): Access and use of Azure database
- [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/): As above
- [FastApi](https://fastapi.tiangolo.com): Allows functions to be called as URL endpoints
- [Python Decouple](https://pypi.org/project/python-decouple/): Allows extraction of .env variables
- [Pytest](https://docs.pytest.org/en/6.2.x/): For unit testing
- [defusedxml](https://pypi.org/project/defusedxml/): XML parser for python
- [uvicorn](https://www.uvicorn.org): ASGI web server implementation
- [python-dateutils](https://pypi.org/project/python-dateutil/): Provides extensions to the datetime module
- [xmltodict](https://pypi.org/project/xmltodict/): For converting XML to dictionaries
- [xmlschema](https://pypi.org/project/xmlschema/): Implementation of XML schema
- [python-magic](https://pypi.org/project/python-magic/): File type identification library

Installation
===============

To get started with the project, follow the steps below:

### Install dependencies

* Run the following in terminal:

```
pip install -r requirements.txt
```

*** Install system dependencies (Magic) ***

* Debian/Ubuntu

```shell
  sudo apt-get install libmagic1
```

* Mac/OSX

```shell
  brew install libmagic
```

4) Install Azure CLI

* Run the following in terminal:
```
brew install azure-cli
```

Running Locally
---------------

To run the pipeline locally, ensure you have the following additional environment variables set:

| Variable                                       | Value                                                | Description                                  |
|------------------------------------------------|------------------------------------------------------|----------------------------------------------|
| BLOB_SERVICE_MODULE                            | "services.blob_service.local.BlobServiceLocal"       | Blob service, set to local subclass          |
| COSMOS_SERVICE_MODULE                          | "services.cosmos_service.local.CosmosServiceLocal"   | Cosmos service, set to local subclass        |
| COSMOS_CLIENT_MODULE                           | "services.cosmos_client.local.CosmosClientLocal"     | Cosmos client service, set to local subclass |
| DATA_SET_SERVICE_MODULE                        | "services.dataset_service.local.DataSetServiceLocal" | Dataset service, set to local subclass       |
| SEARCH_SERVICE_MODULE                          | "services.search_service.local.SearchServiceLocal"   | Search service, set to local subclass        |
| LOCAL_BLOB_PATH                                | "/.local/blob_local/"                                | Path to local blobs                          | 
| LOCAL_COSMOS_CONTAINER_PATH                    | "/.local/containers/"                                |                                              |
| INGESTION_API                                  | "3.0.0"                                              |                                              |
| BLOB_ACCOUNT_NAME                              | < find on azure >                                    |                                              |
| BLOB_ACCOUNT_KEY                               | < find on azure >                                    |                                              |
| BLOB_HESA_CONTAINER_NAME                       | "hesa-raw-xml-ingest"                                |                                              |
| BLOB_HESA_BLOB_NAME                            | "hesa-raw-xml-ingest.xml"                            |                                              |
| COSMOS_DATABASE_ID                             | "discoveruni"                                        |                                              |
| COSMOS_DATABASE_URI                            | ""                                                   |                                              |
| COSMOS_DATABASE_KEY                            | ""                                                   |                                              |
| COSMOS_COLLECTION_COURSES                      | "courses"                                            |                                              |
| COSMOS_COLLECTION_DATASET                      | "datasets"                                           |                                              |
| COSMOS_COLLECTION_INSTITUTIONS                 | "institutions"                                       |                                              |
| COSMOS_COLLECTION_SUBJECTS                     | "subjects"                                           |                                              |
| BLOB_JSON_FILES_CONTAINER_NAME                 | "jsonfiles"                                          |                                              |
| BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_EN       | ""                                                   |                                              |
| BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_CY       | ""                                                   |                                              |
| BLOB_INSTITUTIONS_SITEMAPS_JSON_FILE_BLOB_NAME | "institutions-sitemaps.json"                         |                                              |
| BLOB_VERSION_JSON_FILE_BLOB_NAME               | "version.json"                                       |                                              |
| BLOB_WELSH_UNIS_CONTAINER_NAME                 | "welsh-unis"                                         |                                              |
| BLOB_WELSH_UNIS_BLOB_NAME                      | "welsh_unis.csv"                                     |                                              |
| BLOB_QUALIFICATIONS_CONTAINER_NAME             | "qualifications"                                     |                                              |
| BLOB_QUALIFICATIONS_BLOB_NAME                  | "qualifications.csv"                                 |                                              |
| BLOB_POSTCODES_CONTAINER_NAME                  | "postcodes"                                          |                                              |
| BLOB_POSTCODES_BLOB_NAME                       | "postcodes.csv"                                      |                                              |
| BLOB_SUBJECTS_CONTAINER_NAME                   | "subjects"                                           |                                              |
| BLOB_SUBJECTS_BLOB_NAME                        | "subjects.csv"                                       |                                              |
| BLOB_SUBJECTS_JSON_BLOB_NAME                   | "subjects.json"                                      |                                              |
| BLOB_STORAGE_URL                               | "/"                                                  |                                              |
| STORAGE_URL                                    | ""                                                   |                                              |
| MINUTES_WAIT_BEFORE_CREATE_NEW_DATASET         | "0"                                                  |                                              |
| SEARCH_API_KEY                                 | ""                                                   |                                              |
| SEARCH_URL                                     | "localhost:8000"                                     |                                              |
| SEARCH_API_VERSION                             | "1"                                                  |                                              |
| SEND_GRID_API_KEY                              | ""                                                   |                                              |
| SEND_GRID_FROM_EMAIL                           | ""                                                   |                                              |
| SEND_GRID_TO_EMAILS                            | ""                                                   |                                              |
| POSTCODE_INDEX_NAME                            | ""                                                   |                                              |
| ENVIRONMENT                                    | ""                                                   |                                              |
| STOP_ETL_PIPELINE_ON_WARNING                   | ""                                                   |                                              |



Some of these environment variable used to be known by a different name that has since been updated

Below the new environment variable names have been mapped to their historical counterparts:

| Variable                                       | Historical Name                            |
|------------------------------------------------|--------------------------------------------|
| INGESTION_API                                  |                                            |
| BLOB_ACCOUNT_NAME                              |                                            |
| BLOB_ACCOUNT_KEY                               |                                            |
| BLOB_HESA_CONTAINER_NAME                       | AzureStorageHesaContainerName              |
| BLOB_HESA_BLOB_NAME                            | AzureStorageHesaBlobName                   |
| COSMOS_DATABASE_ID                             | AzureCosmosDbDatabaseId                    |
| COSMOS_DATABASE_URI                            | AzureCosmosDbUri                           |
| COSMOS_DATABASE_KEY                            | AzureCosmosDbKey                           |
| COSMOS_COLLECTION_COURSES                      | AzureCosmosDbCoursesCollectionId           |
| COSMOS_COLLECTION_DATASET                      | AzureCosmosDbDataSetCollectionId           |
| COSMOS_COLLECTION_INSTITUTIONS                 | AzureCosmosDbInstitutionsCollectionId      |
| COSMOS_COLLECTION_SUBJECTS                     | AzureCosmosDbSubjectsCollectionId          |
| BLOB_JSON_FILES_CONTAINER_NAME                 | AzureStorageJSONFilesContainerName         |
| BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_EN       | AzureStorageInstitutionsENJSONFileBlobName |
| BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_CY       | AzureStorageInstitutionsCYJSONFileBlobName |
| BLOB_INSTITUTIONS_SITEMAPS_JSON_FILE_BLOB_NAME | AzureStorageInstitutionsSitemapsBlobName   |
| BLOB_VERSION_JSON_FILE_BLOB_NAME               |                                            |
| BLOB_WELSH_UNIS_CONTAINER_NAME                 | AzureStorageWelshUnisContainerName         |
| BLOB_WELSH_UNIS_BLOB_NAME                      | AzureStorageWelshUnisBlobName              |
| BLOB_QUALIFICATIONS_CONTAINER_NAME             | AzureStorageQualificationsContainerName    |
| BLOB_QUALIFICATIONS_BLOB_NAME                  | AzureStorageQualificationsBlobName         |
| BLOB_POSTCODES_CONTAINER_NAME                  | AzureStoragePostcodesContainerName         |
| BLOB_POSTCODES_BLOB_NAME                       | AzureStoragePostcodesBlobName              |
| BLOB_SUBJECTS_CONTAINER_NAME                   | AzureStorageSubjectsContainerName          |
| BLOB_SUBJECTS_BLOB_NAME                        | AzureStorageSubjectsBlobName               |
| BLOB_SUBJECTS_JSON_BLOB_NAME                   | AzureStorageSubjectsJSONFileBlobName       |
| BLOB_STORAGE_URL                               |                                            |
| STORAGE_URL                                    | StorageUrl                                 |
| MINUTES_WAIT_BEFORE_CREATE_NEW_DATASET         | TimeInMinsToWaitBeforeCreateNewDataSet     |
| SEARCH_API_KEY                                 | SearchAPIKey                               |
| SEARCH_URL                                     | SearchURL                                  |
| SEARCH_API_VERSION                             | AzureSearchAPIVersion                      |
| SEND_GRID_API_KEY                              | SendGridAPIKey                             |
| SEND_GRID_FROM_EMAIL                           | SendGridFromEmail                          |
| SEND_GRID_TO_EMAILS                            | SendGridToEmailList                        |
| POSTCODE_INDEX_NAME                            | PostcodeIndexName                          |
| ENVIRONMENT                                    | Environment                                |
| STOP_ETL_PIPELINE_ON_WARNING                   | StopEtlPipelineOnWarning                   |

Navigate to the root directory and run:

`uvicorn main:app --reload`

Endpoints should be called in the following order:

- /CreateDataSet
- /CreateInst - ensure the Welsh unis CSV is present (BLOB_WELSH_UNIS_BLOB_NAME)
- /SubjectBuilder - ensure the subjects CSV is present (BLOB_SUBJECTS_BLOB_NAME)
- /EtlPipeline - ensure the ingestion XML is present (BLOB_HESA_BLOB_NAME)

Outputs will be visible in the local cosmos container path
set in the environment variable. If local output directories do not exist when the service is run,
they will be created according to the environment variables.

N.B. The search endpoints, /CourseSearchBuilder and /PostcodeSearchBuilder
currently do not provide outputs.

Running tests
-------------

To run the test suite, run the following commands

```
pip install pytest (if you haven't already installed dependencies using the requirements.txt file)
pytest -v
```

Set the -s flag if you would like the print statements to be output as well during debugging

```
pytest -s
```

Getting setting
----------------

```
func azure functionapp fetch-app-settings <app-name>
func settings decrypt
```


