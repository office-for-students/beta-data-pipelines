import json
from typing import Any

from services.dataset_service.base import DataSetServiceBase


class DataSetServiceLocal(DataSetServiceBase):

    def update_status(self, item: str, value: str, updated_at: str = None) -> None:
        print("status updated")

    def get_latest_doc(self) -> dict[str, Any]:
        print("latest doc retrieved")
        return {}

    def get_latest_version_number(self) -> int:
        print("latest version number retrieved")
        return 0

    def query_items(self, query: str) -> list[dict[str, Any]]:
        print("items queried")
        return []

    def create_item(self, dataset_doc: dict[str, Any]) -> None:
        # self.container
        with open(self.container.container_json, 'w') as file:
            json.dump(dataset_doc, file)

        print(f"item created, {self.container.container_id} - {self.container.container_json}")

    def have_all_builds_succeeded(self) -> bool:
        print("checked whether builds have succeeded")
        return False
