from typing import Any
from typing import Optional


class BlobServiceBase:
    def __init__(self, service_string: str = None):
        self.blob_service_string = service_string

    def get_service_client(self) -> Optional[Any]:
        NotImplementedError("This method must be implemented by subclasses")
        return None

    def get_str_file(self, container_name: str, blob_name: str) -> str:
        NotImplementedError("This method must be implemented by subclasses")
        return ""

    def write_stream_file(self, container_name: str, blob_name: str, encoded_file: bytes) -> None:
        NotImplementedError("This method must be implemented by subclasses")


