import json
import os
from typing import Any

from constants import COSMOS_COLLECTION_INSTITUTIONS
from constants import COSMOS_COLLECTION_SUBJECTS
from services.cosmos_client.local import CosmosClientLocal
from services.cosmos_service.base import CosmosServiceBase


class ScriptsLocal:

    @staticmethod
    def execute_stored_procedure(sproc: str, params: list[dict[str, Any]], partition_key: str) -> None:
        # print(f"Executing stored procedure {sproc} with params {params} and partition key {partition_key}")
        print(f"Executing stored procedure {sproc}")


class ContainerLocal:
    def __init__(self, container_id: str):
        self.container_id = container_id
        self.container_json = os.getcwd() + "/.local/containers/" + container_id + ".json"
        self.scripts = ScriptsLocal()
        with open(self.container_json, 'r') as json_file:
            self.json_data = json.load(json_file)

    def query_items(self, query: str, enable_cross_partition_query: bool = False) -> list[dict[str, Any]]:
        print(f"Executing query {query} with cross partition query {'enabled' if enable_cross_partition_query else 'disabled'}")
        if query.startswith("SELECT * from c WHERE c.version = "):
            return self.json_data
        else:
            return []

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
