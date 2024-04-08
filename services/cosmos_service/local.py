import json
import os
from typing import Any

from services.cosmos_client.local import CosmosClientLocal
from services.cosmos_service.base import CosmosServiceBase


class ScriptsLocal:
    def __init__(self, container_id, storage_location: str):
        self.container_id = container_id
        self.storage_location = storage_location

    def execute_stored_procedure(self, sproc: str, params: list[dict[str, Any]], partition_key: str) -> None:
        print(
            f"Executing stored procedure {sproc} with params {params} "
            f"and partition key {partition_key} collection = {self.container_id}"
        )
        # print(f"Executing stored procedure {sproc}")
        with open(self.storage_location, 'r') as json_file:
            try:
                json_data = json.load(json_file)
            except json.decoder.JSONDecodeError:
                # file is empty that's all
                json_data = {}

        if dict == type(json_data) and json_data.get(partition_key) is not None:
            json_data[partition_key] = json_data[partition_key] + params[0]
        elif dict == type(json_data):
            json_data[partition_key] = params[0]
        else:
            json_data = {partition_key: params[0]}
        with open(self.storage_location, 'w') as file:
            json.dump(json_data, file)


class ContainerLocal:
    def __init__(self, container_id: str):
        self.container_id = container_id
        self.root = os.getcwd() + "/.local/containers/"
        self.container_json = self.root + container_id + ".json"
        # passing container_id to use as file reference when storing things locally
        self.scripts = ScriptsLocal(
            container_id=container_id,
            storage_location=self.container_json
        )  # self.scripts is an AWS function, that we are overwriting.
        with open(self.container_json, 'r') as json_file:
            try:
                self.json_data = json.load(json_file)
            except json.decoder.JSONDecodeError:
                # file is empty that's all
                self.json_data = []

    def query_items(self, query: str, enable_cross_partition_query: bool = False) -> list[dict[str, Any]]:
        print(
            f"Executing query {query} with cross partition query {'enabled' if enable_cross_partition_query else 'disabled'}")
        version = query.split(" ")[-1]
        if query.startswith("SELECT * from c WHERE c.version = "):
            if isinstance(self.json_data, dict):
                return self.json_data.get(version)
            else:
                return self.json_data[int(version)]
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
