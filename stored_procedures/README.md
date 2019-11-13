create feedback reports
=================
Program to create feedback reports.

### Configuration Settings

Before running the script, set the following environment variables.

| Variable                            | Default                | Description                                              |
| ----------------------------------- | ---------------------- | -------------------------------------------------------- |
| AzureCosmosDbDatabaseId | retrieve from portal | The id of a CosmosDB database that contains the container to switch the stored procedure is to be added to|
| AzureCosmosDbCollectionId | <container name> | the container name the stored procedure is to be added to |
| AzureCosmosDbUri | retrieve from portal | the uri to CosmosDB |
| AzureCosmosDbKey | retrieve from portal | the key to authenticate to CosmosDB |


### Setup

* Ensure python 3.6 ot greater is installed
* Create a Python virtualenv to run this tool in
* Activate the virtualenv

```
pip install -r requirements.txt
```

### Running the report generator

1. Created a file called setup_env_vars.sh as shown below:

export AzureCosmosDbUri="<uri>"
export AzureCosmosDbKey="<connection_key>"
export AzureCosmosDbDatabaseId="<database_id>"
export AzureCosmosDbCollectionId="<collection_id>"

NB you should use the filename setup_env_vars.sh as it is in .gitignore for this
repo to help prevent sensitive config being accidentally committed and pushed
to github.

2. Run 
```
source setup_env_vars.sh
```

3. Run 
```
python create_stored_procedure.py
```

