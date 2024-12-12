from azure.cosmos import ContainerProxy
from azure.cosmos import CosmosClient

from constants import COSMOS_DATABASE_ID
from services.cosmos_service.base import CosmosServiceBase


class CosmosService(CosmosServiceBase):
    """
    CosmosDB class to handle all interactions with CosmosDB
    """

    def get_container(self, container_id: str) -> ContainerProxy:
        return self.database.get_container_client(container_id)

    @staticmethod
    def get_collection_link(container_id: str) -> str:
        """
        Create and return collection link based on object parameters.

        :return: Link to collection for the CosmosService object's database and collection IDs
        :rtype: str
        """
        return f"dbs/{COSMOS_DATABASE_ID}/colls/{container_id}"

    @classmethod
    def get_ofs_cosmos_service(cls, client: CosmosClient):
        database = client.get_database_client(COSMOS_DATABASE_ID)
        return cls(cosmos_database=database)
