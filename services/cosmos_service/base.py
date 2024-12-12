from typing import Union

from azure.cosmos import ContainerProxy
from azure.cosmos import CosmosClient
from azure.cosmos import DatabaseProxy

from services.cosmos_client.local import CosmosDatabaseLocal


class CosmosServiceBase:
    """
    CosmosDB class to handle all interactions with CosmosDB
    """
    def __init__(self, cosmos_database: Union[DatabaseProxy, CosmosDatabaseLocal]) -> None:
        self.database = cosmos_database

    def get_container(self, container_id: str) -> Union[ContainerProxy, type['ContainerLocal']]:
        NotImplementedError("This method must be implemented by subclasses")

    @staticmethod
    def get_collection_link(container_id: str) -> str:
        NotImplementedError("This method must be implemented by subclasses")
        return ""

    @classmethod
    def get_ofs_cosmos_service(cls, client: CosmosClient):
        NotImplementedError("This method must be implemented by subclasses")
