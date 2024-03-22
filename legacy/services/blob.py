import gzip
import io
from datetime import datetime
from typing import Dict
from typing import Optional

from azure.storage.blob import BlobServiceClient

from constants import BLOB_TEST_BLOB_DIRECTORY


class BlobServiceBase:
    def __init__(self, blob_service_client: BlobServiceClient = None, blob: Optional[Dict[str, str]] = None) -> None:
        self.blob_service_client = blob_service_client
        self.blob = blob

    def get_destination_blob_name(self) -> str:
        """
        Retrieves the destination blob name using the file name and current date

        :return: Name of destination blob
        :rtype: str
        """
        blob_filename = self.blob.name.split("/")[1]
        datetime_str = datetime.today().strftime("%Y%m%d-%H%M%S")
        return f"{datetime_str}-{blob_filename}"

    def get_str_file(self, container_name: str, blob_name: str) -> str:
        NotImplementedError("This method must be implemented by subclasses")

    def write_stream_file(self, container_name: str, blob_name: str, encoded_file: bytes) -> None:
        NotImplementedError("This method must be implemented by subclasses")


class BlobService(BlobServiceBase):
    """Blob service to be used when not running locally"""

    def get_str_file(self, container_name: str, blob_name: str) -> str:
        """
        Retrieves string file from the passed blob and passed container.

        :param container_name: Container name with the blob from which to retrieve string
        :type container_name: str
        :param blob_name: Name of blob from which to retrieve string
        :type blob_name: str
        :return: Retrieved string file
        :rtype: str
        """
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        response = blob_client.download_blob()

        compressed_file = io.BytesIO(response.readall())

        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        decompressed_file = compressed_gzip.read()

        compressed_file.close()
        compressed_gzip.close()

        file_string = decompressed_file.decode("utf-8-sig")

        return file_string

    def write_stream_file(self, container_name: str, blob_name: str, encoded_file: bytes) -> None:
        """
        Writes the passed stream file to the passed blob in the passed container.

        :param container_name: Name of container containing blob to write file to
        :type container_name: str
        :param blob_name: Name of blob to write file to
        :type blob_name: str
        :param encoded_file: File to write to blob
        :type encoded_file: bytes
        :return: None
        """
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(encoded_file, overwrite=True)


class BlobServiceLocal(BlobServiceBase):
    """Blob service to be used when running locally (i.e. for testing purposes)"""

    def __init__(self, azure_storage_connection_string: str = None, blob=None) -> None:
        super().__init__()
        self.blob_path = BLOB_TEST_BLOB_DIRECTORY

    def get_str_file(self, container_name: str, blob_name: str) -> str:
        """
        Retrieves string file from the passed blob and passed container.

        :param container_name: Container name with the blob from which to retrieve string
        :type container_name: str
        :param blob_name: Name of blob from which to retrieve string
        :type blob_name: str
        :return: Retrieved string file
        :rtype: str
        """
        file_path = self.blob_path + "/" + container_name + "/" + blob_name
        with open(file_path, "r") as file:
            file_lines = file.readlines()

        file_string = "\n".join(file_lines)
        return file_string

    def write_stream_file(self, container_name: str, blob_name: str, encoded_file: bytes) -> None:
        """
        Writes the passed stream file to the passed blob in the passed container.

        :param container_name: Name of container containing blob to write file to
        :type container_name: str
        :param blob_name: Name of blob to write file to
        :type blob_name: str
        :param encoded_file: File to write to blob
        :type encoded_file: bytes
        :return: None
        """
        file_path = self.blob_path + "/" + container_name + "/" + blob_name
        with open(file_path, "w") as file:
            file.write(encoded_file.decode("utf-8-sig"))
