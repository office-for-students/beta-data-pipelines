CourseSearchBuilderBlobTrigger - Python
=========================================
The course search builder creates and populates a new version of the course dataset into azure search.

### CourseSearchBuilder function processing steps

1. Course search builder is triggered when a new blob is stored in a storage container. It takes the path to the file and
extracts the dataset version from the filename in path.

2. Using the version number it attempts to delete any existing index for that version before creating a new index in azure search.

3. Following the creation of a new index, the function retrieves all course documents stored in cosmos db for that version and stores them in the new azure search instance.

4. TODO On success the function sends an update to the dataset resource for that version signalling the completion of the creation of search index.

### Configuration Settings

Add the following to your local.settings.json:

| Variable                            | Default                | Description                                              |
| ----------------------------------- | ---------------------- | -------------------------------------------------------- |
| FUNCTIONS_WORKER_RUNTIME            | python                 | The programming language the function worker runs on     |
| AzureCosmosDbUri                    | {retrieve from portal} | The uri to the cosmosdb instance                         |
| AzureCosmosDbKey                    | {retrieve from portal} | The database key to access cosmosdb instance             |
| AzureCosmosDbDatabaseId             | discoverUni            | The name of the cosmosdb database                        |
| AzureCosmosDbUkRlpCollectionId      | courses                | The name of the courses collection/container in cosmosdb |
| AzureStorageAccountConnectionString | {retrieve from portal} | The connection string to access storage account          |
| AzureWebJobsStorage                 | {retrieve from portal} | The default endpoint to access storage account           |
| StopEtlPipelineOnWarning            | false                  | Boolean flag to stop function worker on a warning        |
| CourseSearchBuilerContainerName     | course-search-blobs    | The storage container that will trigger the function     |
| SearchURL                           | {retrieve from portal} | The uri to the azure search instance                     |
| SearchAPIKey                        | {retrieve from portal} | The api key to access the azure search instance          |
| AzureSearchAPIVersion               | 2019-05-06             | The azure search API version for instance                |
