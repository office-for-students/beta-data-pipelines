Documentation
=============
The documentation for this project is built using [Sphinx](#getting-started).

Documentation should be built by installing the requirements and then the following commands:

```
cd docs
make clean html
```

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


Installation
------------

To get started with the project, follow the steps below:

1) Clone the repo

* Run the following in terminal:

```
git clone <repo>
```

2) Install dependencies

* Run the following in terminal:

```
pip install -r requirements.txt
```

4) Install Azure CLI

* Run the following in terminal:
```
brew install azure-cli
```

Running Locally
---------------

To run the pipeline locally, ensure you have the following additional environment variables set:

| Variable                    | Value                                                | Description                                  |
|-----------------------------|------------------------------------------------------|----------------------------------------------|
| BLOB_SERVICE_MODULE         | "services.blob_service.local.BlobServiceLocal"       | Blob service, set to local subclass          |
| COSMOS_SERVICE_MODULE       | "services.cosmos_service.local.CosmosServiceLocal"   | Cosmos service, set to local subclass        |
| COSMOS_CLIENT_MODULE        | "services.cosmos_client.local.CosmosClientLocal"     | Cosmos client service, set to local subclass |
| DATA_SET_SERVICE_MODULE     | "services.dataset_service.local.DataSetServiceLocal" | Dataset service, set to local subclass       |
| SEARCH_SERVICE_MODULE       | "services.search_service.local.SearchServiceLocal"   | Search service, set to local subclass        |
| LOCAL_BLOB_PATH             | "/.local/blob_local/"                                | Path to local blobs                          | 
| LOCAL_COSMOS_CONTAINER_PATH | "/.local/containers/"                                | Path to local cosmos containers              |

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


