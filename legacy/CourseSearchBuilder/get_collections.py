from legacy.services.dataset_service import DataSetService
from legacy.services.utils import get_collection_link
from legacy.services.utils import get_cosmos_client


def get_collections(cosmos_field_db_id):
    version = DataSetService().get_latest_version_number()
    cosmos_db_client = get_cosmos_client()
    collection_link = get_collection_link(cosmos_field_db_id)

    query = f"SELECT * from c where c.version = {version}"

    options = {"enableCrossPartitionQuery": True}

    collections_list = list(cosmos_db_client.QueryItems(collection_link, query, options))

    return collections_list

def get_institutions():
    return get_collections("COSMOS_COLLECTION_INSTITUTIONS")

