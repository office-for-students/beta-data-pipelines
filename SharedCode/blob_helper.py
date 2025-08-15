import gzip
import io
import os
from datetime import datetime

from azure.storage.blob import BlobServiceClient


class BlobHelper:
    def __init__(self, blob=None):
        self.blob_service = BlobServiceClient.from_connection_string(
            os.environ["AzureStorageAccountConnectionString"]
        )
        self.blob = blob

    def create_output_blob(self, destination_container_name):
        source_url = os.environ["StorageUrl"] + self.blob.name
        destination_blob_name = self.get_destination_blob_name()
        dest_blob_client = self.blob_service.get_blob_client(destination_container_name, destination_blob_name)
        copy_operation = dest_blob_client.start_copy_from_url(source_url)
        print(f"Copy status: {copy_operation['copy_status']}")

    def get_destination_blob_name(self):
        blob_filename = self.blob.name.split("/")[1]
        datetime_str = datetime.today().strftime("%Y%m%d-%H%M%S")
        return f"{datetime_str}-{blob_filename}"

    def get_str_file(self, storage_container_name, storage_blob_name):
        # Get the blob client
        blob_client = self.blob_service.get_blob_client(container=storage_container_name, blob=storage_blob_name)

        # Download the blob as a stream
        compressed_stream = blob_client.download_blob().readall()

        # Decompress the gzip file in memory
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_stream), mode='rb') as decompressed_file:
            file_string = decompressed_file.read().decode("utf-8-sig", errors="ignore")  # Assuming text data

        return file_string

    def write_stream_file(self, storage_container_name, storage_blob_name, encoded_file):
        blob_client = self.blob_service.get_blob_client(container=storage_container_name, blob=storage_blob_name)
        blob_client.upload_blob(encoded_file, overwrite=True)  # `overwrite=True` replaces existing blob