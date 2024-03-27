import gzip
import io
from typing import Optional

from azure.storage.blob import BlobServiceClient

from services.blob_service.base import BlobServiceBase


class BlobService(BlobServiceBase):
    """Blob service to be used when not running locally"""

    def get_service_client(self) -> Optional[BlobServiceClient]:
        return BlobServiceClient.from_connection_string(self.blob_service_string)

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
        blob_client = self.get_service_client().get_blob_client(container=container_name, blob=blob_name)
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
        blob_client = self.get_service_client().get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(encoded_file, overwrite=True)


