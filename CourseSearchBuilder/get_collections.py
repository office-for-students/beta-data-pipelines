import os

from SharedCode.dataset_helper import DataSetHelper
from SharedCode.utils import get_cosmos_client


def get_collections(collection_id_env):
    version = DataSetHelper().get_latest_version_number()
    cosmos_db_client = get_cosmos_client()
    database_id = os.environ.get('AzureCosmosDbDatabaseId')
    collection_id = os.environ.get(collection_id_env)

    database = cosmos_db_client.get_database_client(database_id)
    container = database.get_container_client(collection_id)

    query = f"SELECT * from c where c.version = {version}"

    options = {"enable_cross_partition_query": True}

    collections_list = list(container.query_items(query=query, **options))

    return collections_list

def get_institutions():
    return get_collections("AzureCosmosDbInstitutionsCollectionId")