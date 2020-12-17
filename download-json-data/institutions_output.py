#!/usr/bin/env python
import os
import json
from SharedCode.utils import get_courses_by_version
import azure.cosmos.cosmos_client as cosmos_client

cosmos_db_client = cosmos_client.CosmosClient(
    url_connection=os.environ["AzureCosmosDbUri"],
    auth={"masterKey": os.environ["AzureCosmosDbKey"]}
)
collection_link = "dbs/" + os.environ["AzureCosmosDbDatabaseId"] + "/colls/" + \
                  os.environ["AzureCosmosDbInstitutionsCollectionId"]

query = f"SELECT * from c WHERE c.version = 36"

options = {"enableCrossPartitionQuery": True}

course_list = list(
    cosmos_db_client.QueryItems(collection_link, query, options)
)

with open('institutions.json', 'w') as f:
     f.write(json.dumps(course_list, indent=4))
