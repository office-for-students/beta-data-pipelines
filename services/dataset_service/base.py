from typing import Any
from typing import Union

from azure.cosmos import ContainerProxy

from services.cosmos_service.local import ContainerLocal


class DataSetServiceBase:

    def __init__(self, cosmos_container: Union[ContainerProxy, ContainerLocal]):
        self.container = cosmos_container

    def update_status(self, item: str, value: str, updated_at: str = None) -> None:
        NotImplementedError("This method must be implemented by subclasses")

    def get_latest_doc(self) -> dict[str, Any]:
        NotImplementedError("This method must be implemented by subclasses")
        return {}

    def get_latest_version_number(self) -> int:
        NotImplementedError("This method must be implemented by subclasses")
        return 0

    def query_items(self, query: str) -> list[dict[str, Any]]:
        NotImplementedError("This method must be implemented by subclasses")
        return []

    def create_item(self, dataset_doc: dict[str, Any]) -> None:
        NotImplementedError("This method must be implemented by subclasses")

    def have_all_builds_succeeded(self) -> bool:
        NotImplementedError("This method must be implemented by subclasses")
        return False
