from azure.cosmos import CosmosClient

from constants import KEY_COSMOS_MASTER_KEY


# database_id = "discoveruni"
# container_id = "datasets"


class CosmosService:
    """
    CosmosDB class to handle all interactions with CosmosDB
    """

    def __init__(self, cosmosdb_uri: str, cosmosdb_key: str, database_id: str, container_id: str) -> None:
        """
        Initialise CosmosDB class: create CosmosDB client, database and container

        :param cosmosdb_uri: URI for a cosmosdb
        :type cosmosdb_uri: str
        :param cosmosdb_key: Access key for cosmosdb
        :type cosmosdb_key: str
        :param database_id: e.g. 'discoveruni' refers to the database in cosmosdb and is the database_id
        :type database_id: str
        :param container_id: e.g. 'datasets' refers to the database document in cosmosdb and is the container_id
        :type container_id: str
        :return: None
        """
        self.client = CosmosClient(url=cosmosdb_uri, credential={KEY_COSMOS_MASTER_KEY: cosmosdb_key})
        self.database = self.client.get_database_client(database_id)
        self.container = self.database.get_container_client(container_id)
        self.database_id = database_id
        self.container_id = container_id

    def get_collection_link(self) -> str:
        """
        Create and return collection link based on object parameters.

        :return: Link to collection for the CosmosService object's database and collection IDs
        :rtype: str
        """
        return f"dbs/{self.database_id}/colls/{self.container_id}"

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
