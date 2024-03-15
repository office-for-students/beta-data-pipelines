import gzip
import io
from datetime import datetime

from azure.storage.blob import BlobServiceClient


class BlobService:
    def __init__(self, azure_storage_connection_string: str = None, blob=None) -> None:
        self.blob_service_client = BlobServiceClient.from_connection_string(
            azure_storage_connection_string) if azure_storage_connection_string else None
        self.blob = blob

    def get_destination_blob_name(self) -> str:
        blob_filename = self.blob.name.split("/")[1]
        datetime_str = datetime.today().strftime("%Y%m%d-%H%M%S")
        return f"{datetime_str}-{blob_filename}"

    def get_str_file(self, container_name: str, blob_name: str) -> str:
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        response = blob_client.download_blob()

        compressed_file = io.BytesIO(response.readall())

        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        decompressed_file = compressed_gzip.read()

        compressed_file.close()
        compressed_gzip.close()

        file_string = decompressed_file.decode("utf-8-sig")

        return file_string

    def write_stream_file(self, container_name: str, blob_name: str, encoded_file: str) -> None:
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(encoded_file, overwrite=True)
