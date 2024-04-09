import json
import logging
from datetime import datetime
from typing import Any

from services.dataset_service.base import DataSetServiceBase


class DataSetServiceLocal(DataSetServiceBase):

    def update_status(self, item: str, value: str, updated_at: str = None) -> None:
        dataset_doc = self.get_latest_doc()
        try:
            if item == "root":
                dataset_doc["status"] = value
            else:
                dataset_doc["builds"][item]["status"] = value
            dataset_doc["updated_at"] = datetime.utcnow().isoformat()
            if updated_at:
                dataset_doc["updated_at"] = updated_at
        except KeyError:
            logging.error(f"Dataset document does not exist, call /CreateDataSet/ endpoint first")
            return

        with open(self.container.container_json, 'r') as file:
            try:
                json_data = json.load(file)
            except json.decoder.JSONDecodeError:
                json_data = {}
        if isinstance(json_data, dict):
            json_data = [json_data]

        version_to_update = dataset_doc['version']
        for index, doc in enumerate(json_data):
            if doc['version'] == version_to_update:
                json_data[index] = dataset_doc

        with open(self.container.container_json, 'w') as file:
            json.dump(json_data, file)

    def get_latest_doc(self) -> dict[str, Any]:
        latest_version_number = self.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {latest_version_number}"
        dataset_docs = list(self.container.query_items(query, enable_cross_partition_query=True))
        for doc in dataset_docs:
            if int(doc['version']) == int(latest_version_number):
                print(f"latest doc retrieved - version {latest_version_number}")
                return doc

        return {"created_at": "2024-01-01T00:00:00.000000+00:00"}

    def get_latest_version_number(self) -> int:
        with open(self.container.container_json, 'r') as file:
            try:
                json_data = json.load(file)
            except json.decoder.JSONDecodeError:
                json_data = {}

        if json_data:
            if isinstance(json_data, dict):
                return int(json_data['version'])
            else:
                versions = []
                for doc in json_data:
                    versions.append(int(doc['version']))
                return max(versions)
        else:
            return 0

    def query_items(self, query: str) -> list[dict[str, Any]]:
        if query == "SELECT * FROM c ":  # Return number of dataset docs
            with open(self.container.container_json, 'r') as file:
                try:
                    json_data = json.load(file)
                except json.decoder.JSONDecodeError:
                    json_data = {}
            if isinstance(json_data, dict):
                return [json_data]
            else:
                return json_data

        elif query.startswith("SELECT * FROM c WHERE c.version = "):  # Return dict of specified doc
            with open(self.container.container_json, 'r') as file:
                try:
                    json_data = json.load(file)
                except json.decoder.JSONDecodeError:
                    json_data = {"version": 0, "created_at": "2024-01-01T00:00:00.000000+00:00"}
            if isinstance(json_data, dict):
                return [json_data]
            else:  # It's a list
                version = int(query.split(" ")[-1])
                for doc in json_data:
                    if doc["version"] == version:
                        return [doc]

        logging.info("items queried")
        return [{"version": 0}]

    def create_item(self, dataset_doc: dict[str, Any]) -> None:
        # Check if existing data is a list or dictionary. If list, append new doc, otherwise combine existing and
        # new docs into a list
        with open(self.container.container_json, 'r') as file:
            try:
                json_data = json.load(file)
            except json.decoder.JSONDecodeError:
                json_data = None

        if json_data is None:
            json_data = [dataset_doc]
        else:
            if isinstance(json_data, list):
                json_data += [dataset_doc]
            else:
                json_data = [json_data] + [dataset_doc]

        with open(self.container.container_json, 'w') as file:
            json.dump(json_data, file)

        logging.info(f"item created, {self.container.container_id} - {self.container.container_json}")

    def have_all_builds_succeeded(self) -> bool:
        print("checked whether builds have succeeded")
        dataset_doc = self.get_latest_doc()
        build_statuses = [
            dataset_doc["builds"][item]["status"] == "succeeded"
            for item in ("courses", "institutions", "search", "subjects")
        ]
        return all(build_statuses)
