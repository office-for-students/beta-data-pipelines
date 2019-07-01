OfS Beta Serverless Data Pipelines
==================
OfS Beta Serverless Data Ingestion and ETL Pipelines using Azure Functions and the Azure Python SDK

[![Build Status](https://dev.azure.com/nathanshumoogum/pre-prod/_apis/build/status/office-for-students.beta-data-pipelines?branchName=develop)](https://dev.azure.com/nathanshumoogum/pre-prod/_build/latest?definitionId=1&branchName=develop)

### Installation

As the pipeline azure functions are written in Python using the Azure Python SDK and relevant python package dependencies, install python version 3.6.*, the Azure Python SDK currently does not work the version 3.7*.

1) Install [.NET Core SDK 2.2](https://dotnet.microsoft.com/download)

* Continue with [Macbook setup](https://github.com/office-for-students/beta-data-pipelines/tree/feature/fix-xml-validation#macbook-setup)
* Continue with [Windows setup](https://github.com/office-for-students/beta-data-pipelines/tree/feature/fix-xml-validation#windows-setup)

#### Macbook setup

2) Install Python 3.6.8 (current latest stable version that works with Azure client)

Using [Homebrew](https://brew.sh/) to install

* Run the following
```
brew install sashkab/python/python36` or download from [here](https://www.python.org/downloads/release/python-368/)
pip3.6 install -U pip setuptools
```

3) Setting PATH for Python 3.6, the specifies the location of the Python version to use on device

* Copy and paste the following into your `bash_profile`
```
PATH="/usr/local/Cellar/python36/3.6.8_2/bin:${PATH}"
```

4) Install Azure CLient

Run the following in terminal:
```
brew install azure-cli
```

5) Install the core tools package to enable debugging

* Run the following in terminal:
```
brew tap azure/functions
brew install azure-functions-core-tools
```

6) Setup Visual Studio Code and access Azure

* Install [Visual Studio Code](https://code.visualstudio.com/)
* Install the following extensions - Visual Studio Code documentation [here](https://code.visualstudio.com/docs/editor/extension-gallery)
```
Azure Account
Azure Functions
Azure Storage
Azure Storage Explorer
Azure Cosmos DB
```
* Sign into Azure within Visual Studio Code prior to building first azure function, follow from [Sign in to Azure](https://code.visualstudio.com/docs/python/tutorial-azure-functions#_sign-in-to-azure)

7) [Continue with Database set up](https://github.com/office-for-students/beta-data-pipelines/tree/feature/fix-xml-validation#database-setup)

#### Windows setup

TODO

7) [Continue with Database set up](https://github.com/office-for-students/beta-data-pipelines/tree/feature/fix-xml-validation#database-setup)

#### Database Setup

TODO - Setup local Cosmos DB instance and update `local.settings.json` blurb

#### Blob Setup

TODO - Info on Setting up Blob storage containers
The XML in test-data/kis20190507140855-test.xml can be used as HESA dummy
source during development and testing by uploading to the hesa-raw-xml-dummy-source container.

### Configuration

Create or edit `local.settings.json`:
* `vi ~/{PATH to workspace}/local.settings.json`
* Add the following json to file and set configuration values:
```json
{
  "IsEncrypted": false,
  "Values": {
        "FUNCTIONS_WORKER_RUNTIME": "python",
        "AzureCosmosDbUri": "{cosmos db uri}",
        "AzureCosmosDbKey": "{account key value}",
        "AzureCosmosDbConnectionString": "AccountEndpoint={cosmos db uri};AccountKey={account key}",
        "AzureCosmosDbDatabaseId": "{name of database}",
        "AzureCosmosDbCollectionId": "institutions",
        "AzureStorageAccountName": "{storage account name}",
        "AzureStorageAccountKey": "{storage account key}",
        "AzureStorageAccountConnectionString": "DefaultEndpointsProtocol=https;AccountName={storage account name};AccountKey={storage account key};EndpointSuffix=core.windows.net",
        "AzureStorageAccountOutputContainerName": "{storage container name, e.g. hesa-raw-xml-ingest}",
        "AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName={storage account name};AccountKey={storage account key};EndpointSuffix=core.windows.net",
        "OutputBlobNamePrefix": "hesa-raw-xml-",
        "DummyAzureStorageAccountInputContainerName": "hesa-raw-xml-dummy-source",
        "DummyInputBlobName": "kis20190507140855-test.xml",
        "DummyInputBlobUrl": "https://{storage account name}.blob.core.windows.net/hesa-raw-xml-dummy-source/kis20190507140855-test.xml",
        "StopEtlPipelineOnWarning": "false",
        "XsdFilename": "unistatsoutputschema_leo.xsd",
        "XPathInstitution": "INSTITUTION"
  }
}
```

#### Running Service

To run function on a virtual environment, using terminal do the following:
```
cd ~/{PATH to workspace}
python3.6 -m venv .env
source .env/bin/activate
func host start
```

If you would like to run a single function, using the terminal do the following:
```
cd ~/{PATH to workspace}
python3.6 -m venv .env
source .env/bin/activate
cd {function name}
func host start
```

Use `ctrl c` to close function and `deactivate` to exit virtual environment

#### Running tests

To run the test suite, run the following commands

```
pip install pytest (if you haven't already installed dependencies using the requirements.txt file)
pytest -v
```

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

See [LICENSE](LICENSE.md) for details.

### Data mappings (XML to JSON)

See [COURSE](docs/COURSE.md) for mappings of course data
See [COURSE STATISTICS](doc/STATISTICS.md) for mappings for course statistics
See [INSTITUTION](docs/INSTITUTION.md) for mappings of institution data
