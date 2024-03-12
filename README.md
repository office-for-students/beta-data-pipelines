OfS Serverless Data Pipelines
==================
OfS Serverless Data Ingestion and ETL Pipelines using Azure Functions and Azure Cosmos DB

### Table of Contents

* [About the Project](#about-the-project)
* [Getting Started](#getting-started)
    * [Installation](#installation)
    * [Configuration Settings](#configuration-settings)
    * [Running Service](#running-service)
    * [Running tests](#running-tests)

### About the Project

Project leverages Azure Functions to create a serverless data ingestion and ETL pipeline.

The purpose of the project is to create a pipeline that can be used to ingest data from
[HESA data service](https://www.hesa.ac.uk/data-and-analysis/students).
These data pipelines transform the data into a common format and store the data in a datastore that can be used to
power a search engine.

The project is currently being used to power the [Discover Uni](https://discoveruni.gov.uk/) website.

### Getting Started

Code is written in Python 3.11+ and uses:

- [Azure Functions](https://docs.microsoft.com/en-us/azure/azure-functions/)
- [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/)
- [FastApi](https://fastapi.tiangolo.com)
- [Python Decouple](https://pypi.org/project/python-decouple/)
- [Pytest](https://docs.pytest.org/en/6.2.x/)

#### Installation

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

#### Running Service


#### Running tests

To run the test suite, run the following commands

```
pip install pytest (if you haven't already installed dependencies using the requirements.txt file)
pytest -v
```

Set the -s flag if you would like the print statements to be output as well during debugging

```
pytest -s
```

### Contributing

See [CONTRIBUTING](docs/CONTRIBUTING.md) for details.

### License

See [LICENSE](docs/LICENSE.md) for details.

### Data mappings (XML to JSON)

See [COURSE](docs/COURSE.md) for mappings of course data

See [COURSE STATISTICS](docs/STATISTICS.md) for mappings for course statistics

See [DATASET](docs/DATASET.md) for dataset structure

See [INSTITUTION](docs/INSTITUTION.md) for mappings of institution data

##### Getting setting:

`func azure functionapp fetch-app-settings <app-name>`
`func settings decrypt`


