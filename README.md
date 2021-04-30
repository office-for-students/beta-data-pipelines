OfS Beta Serverless Data Pipelines
==================
OfS Beta Serverless Data Ingestion and ETL Pipelines using Azure Functions and the Azure Python SDK

Builds

master - [![Build Status](https://dev.azure.com/ofsbeta/discoverUni/_apis/build/status/prod/prod-data-pipelines?branchName=master)](https://dev.azure.com/ofsbeta/discoverUni/_build/latest?definitionId=13&branchName=master)
develop - [![Build Status](https://dev.azure.com/ofsbeta/discoverUni/_apis/build/status/dev/dev-data-pipelines?branchName=develop)](https://dev.azure.com/ofsbeta/discoverUni/_build/latest?definitionId=7&branchName=develop)

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


### Configuration Settings

Add the following to your local.settings.json:

| Variable                                   | Default                   | Description                                                                                                  |
| ------------------------------------------ | ------------------------- | ------------------------------------------------------------------------------------------------------------ |
| FUNCTIONS_WORKER_RUNTIME                   | python                    | The programming language the function worker runs on                                                         |
| AzureCosmosDbUri                           | {retrieve from portal}    | The cosmos db uri to access the datastore                                                                    |
| AzureCosmosDbKey                           | {retrieve from portal}    | The cosmos database key in which to connect to the datastore                                                 |
| AzureCosmosDbDatabaseId                    | discoveruni               | The name of the database in which resource documents are stored in                                           |
| AzureCosmosDbInstitutionsCollectionId      | institutions              | The name of the collection in which institutions are uploaded to                                             |
| AzureCosmosDbCoursesCollectionId           | courses                   | The name of the collection in which courses are uploaded to                                                  |
| AzureCosmosDbUkRlpCollectionId             | ukrlp                     | The name of the collection in which ukrlp docs are uploaded to                                               |
| AzureCosmosDbUkRlpStaticCollectionId       | ukrlp_static              | The name of the collection in which ukrlp docs are uploaded to                                               |
| AzureCosmosDbSubjectsCollectionId          | subjects                  | The name of the collection in which subjects are uploaded to                                                 |
| AzureCosmosDbDataSetCollectionId           | datasets                  | The name of the collection in which datasets are loaded                                                      |
| AzureSearchAPIVersion                      | 2019-05-06                | The azure search API version for instance                                                                    |
| AzureStorageAccountName                    | {retrieve from portal}    | The name of the storage account instance                                                                     |
| AzureStorageAccountKey                     | {retrieve from portal}    | The key in which to connect to the storage account                                                           |
| AzureStorageAccountConnectionString        | {retrieve from portal}    | The connection string to access storage account                                                              |
| AzureWebJobsStorage                        | {retrieve from portal}    | The default endpoint to access storage account                                                               |
| AzureStorageHesaContainerName              | hesa-raw-xml-ingest       | The name of the storage container in which the latest HESA xml is located                                    |
| AzureStorageSubjectsContainerName          | subjects                  | The name of the storage container in which the latest subjects csv file is located                           |
| AzureStoragePostcodesContainerName         | postcodes                 | The name of the storage container in which the latest postcodes csv file is located                          |
| AzureStorageJSONFilesContainerName         | jsonfiles                 | The name of the storage container in which the json files will be generated                                  |
| AzureStorageQualificationsContainerName    | qualifications            | The name of the storage container in which the latest qualification level csv file is located                |
| AzureStorageWelshUnisContainerName         | welsh-unis                | The name of the storage container in which the latest welsh university csv file is located                   |
| AzureStorageInstitutionsCYJSONFileBlobName | institutions_cy.json      | The name of the storage blob for the welsh institution json file                                             |
| AzureStorageInstitutionsENJSONFileBlobName | institutions_en.json      | The name of the storage blob for the english institution json file                                           |
| AzureStoragePostcodesBlobName              |                           | The name of the storage blob for the postcode file                                                           |
| AzureStorageQualificationsBlobName         | qualification-levels.csv  | The name of the storage blob for the qualification levels file                                               |
| AzureStorageSubjectsBlobName               |                           | The name of the storage blob for the subject labels file                                                     |
| AzureStorageSubjectsJSONFileBlobName       | subjects.json             | The name of the storage blob for the subject json file                                                       |
| AzureStorageWelshUnisBlobName              |                           | The name of the storage blob for the welsh institutions file                                                 |
| DatabaseThroughput                         | 400                       | The throughput (RU/s) for subjects collection                                                                |
| Environment                                |                           | The environment that is running the function                                                                 |
| PostcodeIndexName                          | postcodes                 | The name of the search index for postcodes                                                                   |
| UkRlpUrl                                   | {retrieve from ukrlp}     | The url to the UKRLP API service                                                                             |
| UkRlpOfsId                                 | {retrieve from ukrlp}     | The organisation id calling the UKRLP API, unique to each organisation                                       |
| SearchURL                                  | {retrieve from portal}    | The uri to the azure search instance                                                                         |
| SearchAPIKey                               | {retrieve from portal}    | The api key to access the azure search instance                                                              |
| SendGridAPIKey                             | {retrieve from portal}    | The API key for the SendGrid client                                                                          |
| SendGridEnabled                            |                           | The boolean that defines if automated e-mails are enabled                                                    |
| SendGridFromEmail                          |                           | The address from which SendGrid will send automated e-mails                                                  |
| SendGridFromName                           |                           | The name from used by SendGrid to send automated e-mails                                                     |
| SendGridToEmailList                        |                           | The list used by SendGrid to send automated e-mails, separated by ";"                                        |
| StopEtlPipelineOnWarning                   | false                     | Boolean flag to stop function worker on a warning                                                            |
| StorageUrl                                 | {retrieve from portal}    | The url to the top level storage                                                                             |
| TimeInMinsToWaitBeforeCreateNewDataSet     | 120                       | You may need to reduce this time if you wish to run more frequently -e.g., to retry after a fix              |


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

If you're new here, you can also run the tests by performing the following commands:
```
virtualenv venv -p 3.6
source venv/bin/activate
pip install pytest
pip install -r requirements.txt
pytest -v

# target a specific test
pytest EtlPipeline/tests/test_course_doc.py

# target a test that relies on Azure (relies on exporting local.settings.json)
python EtlPipeline/dev-tools/dev-test-scripts/test-course-docs.py
```

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

See [LICENSE](LICENSE.md) for details.

### Data mappings (XML to JSON)

See [COURSE](docs/COURSE.md) for mappings of course data
See [COURSE STATISTICS](doc/STATISTICS.md) for mappings for course statistics
See [DATASET](doc/DATASET.md) for dataset structure
See [INSTITUTION](docs/INSTITUTION.md) for mappings of institution data
