from services.cosmos_client.local import CosmosClientLocal
from services.cosmos_service.base import CosmosServiceBase


class ContainerLocal:
    def __init__(self, container_id: str):
        self.container_id = container_id


class CosmosServiceLocal(CosmosServiceBase):
    """
    CosmosDB class to handle all interactions with CosmosDB
    """

    def get_container(self, container_id: str) -> ContainerLocal:
        print("container retrieved")
        return ContainerLocal(container_id=container_id)

    @staticmethod
    def get_collection_link(container_id: str) -> str:
        print("collection link retrieved")
        return ""

    @classmethod
    def get_ofs_cosmos_service(cls, client: CosmosClientLocal):
        database = client.get_database_client("local_db_id")
        return CosmosServiceLocal(cosmos_database=database)
