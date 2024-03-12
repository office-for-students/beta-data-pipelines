import logging

from azure.cosmos import CosmosClient

database_id = "discoveruni"
container_id = "datasets"

class CosmosService:
    """
    CosmosDB class to handle all interactions with CosmosDB
    """

    def __init__(self, cosmosdb_uri: str, cosmosdb_key: str, database_id: str, container_id: str):
        """
        Initialise CosmosDB class: create CosmosDB client, database and container

        :param cosmosdb_uri: URI for a cosmosdb
        :param cosmosdb_key: Access key for cosmosdb
        :param database_id: e.g. 'discoveruni' refers to the database in cosmosdb and is the database_id
        :param container_id: e.g. 'datasets' refers to the database document in cosmosdb and is the container_id

        """
        self.client = CosmosClient(cosmosdb_uri, cosmosdb_key)
        self.database = self.client.get_database_client(database_id)
        self.container = self.database.get_container_client(container_id)

    def get_highest_successful_version_number(self):
        query = "SELECT VALUE MAX(c.version) from c WHERE c.status = 'succeeded'"
        max_version_number_list = list(
            self.container.query_items(
                query=query,
                enable_cross_partition_query=True,
                max_item_count=1
            )
        )
        version = max_version_number_list[0]
        logging.info(f"Highest successful dataset version: {version}")
        return version

    @staticmethod
    def get_collection_link(db_id: str, collection_id: str) -> str:
        """
        Create and return collection link based on values passed in

        :param db_id:
        :type db_id:
        :param collection_id:
        :type collection_id:
        :return:
        :rtype:
        """

        return f"dbs/{db_id}/colls/{collection_id}"