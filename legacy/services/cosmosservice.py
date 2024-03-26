from azure.cosmos import ContainerProxy
from azure.cosmos import CosmosClient
from azure.cosmos import DatabaseProxy

from constants import COSMOS_DATABASE_ID


# database_id = "discoveruni"
# container_id = "datasets"


class CosmosService:
    """
    CosmosDB class to handle all interactions with CosmosDB
    """

    def __init__(self, cosmos_database: DatabaseProxy) -> None:
        # cosmosdb_uri: str, cosmosdb_key: str
        self.database = cosmos_database

    def get_container(self, container_id: str) -> ContainerProxy:
        return self.database.get_container_client(container_id)

    @staticmethod
    def get_collection_link(container_id) -> str:
        """
        Create and return collection link based on object parameters.

        :return: Link to collection for the CosmosService object's database and collection IDs
        :rtype: str
        """
        return f"dbs/{COSMOS_DATABASE_ID}/colls/{container_id}"

    @classmethod
    def get_ofs_cosmos_service(cls, client: CosmosClient):
        client = client
        database = client.get_database_client(COSMOS_DATABASE_ID)
        return cls.__init__(cosmos_database=database)

    # This specific implementation does not appear to be used
    # def get_highest_successful_version_number(self) -> int:
    #     """
    #     Retrieves the version number of the latest successful ingestion
    #
    #     :return: Highest successful version number
    #     :rtype: int
    #     """
    #     query = "SELECT VALUE MAX(c.version) from c WHERE c.status = 'succeeded'"
    #     max_version_number_list = list(
    #         self.container.query_items(
    #             query=query,
    #             enable_cross_partition_query=True,
    #             max_item_count=1
    #         )
    #     )
    #     version = max_version_number_list[0]
    #     logging.info(f"Highest successful dataset version: {version}")
    #     return version
