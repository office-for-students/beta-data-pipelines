from SharedCode.blob_helper import BlobHelper
from SharedCode.dataset_helper import DataSetHelper
from SharedCode.utils import get_cosmos_client, get_collection_link


def get_collections(cosmos_field_db_id):
    version = DataSetHelper().get_latest_version_number()
    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(
        "AzureCosmosDbDatabaseId", cosmos_field_db_id
    )

    query = f"SELECT * from c where c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    collections_list = list(cosmos_db_client.QueryItems(collection_link, query, options))

    return collections_list

def get_institutions():
    return get_collections("AzureCosmosDbInstitutionsCollectionId")

