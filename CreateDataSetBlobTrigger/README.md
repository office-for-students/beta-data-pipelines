# CreateDataSetBlobTrigger - Python
The service responsible for creating new datasets.

### CreateDataSetBlobTrigger function processing steps

1. The dataset service is triggered when a new blob is stored in the storage container it is configured for.

2. Decompresses the input data and checks that it is well formed XML.

3. Checks that the previous dataset was created at least a configurable number of minutes ago. If not, it reports an error and stops.

4. Creates a new dataset and increments the version so it is one higher than the previous highest dataset version.

5. Copies the input data to the storage container that triggers CreateUkrlp service.

### Configuration Settings

Add the following to your local.settings.json:

| Variable                                   | Default                | Description                                              |
| ------------------------------------------ | ---------------------- | -------------------------------------------------------- |
| FUNCTIONS_WORKER_RUNTIME                   | python                 | The programming language the function worker runs on     |
| AzureCosmosDbUri                           | {retrieve from portal} | The uri to the cosmosdb instance                         |
| AzureCosmosDbKey                           | {retrieve from portal} | The database key to access cosmosdb instance             |
| AzureCosmosDbDatabaseId                    | discoverUni            | The name of the cosmosdb database                        |
| AzureCosmosDbDataSetCollectionId           | datasets               | The name of the collection in which datasets are loaded  |
| AzureStorageAccountConnectionString        | {retrieve from portal} | The connection string to access storage account          |
| StorageUrl                                 | {retrieve from portal} | The url to the top level storage                         |
| TimeInMinsToWaitBeforeCreateNewDataSet     | 120                    | Time in minutes before new dataset can be created        |
| UkrlpInputContainerName                    | ukrlp-input            | The storage container UKRLP is triggered from            |
