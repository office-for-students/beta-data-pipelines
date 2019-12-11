import gzip
import os
import io

from datetime import datetime
from azure.storage.blob import BlockBlobService


class BlobHelper:
    def __init__(self, blob=None):
        account_name = os.environ["AzureStorageAccountName"]
        account_key = os.environ["AzureStorageAccountKey"]
        self.blob_service = BlockBlobService(
            account_name=account_name, account_key=account_key
        )
        self.blob = blob

    def create_output_blob(self, destination_container_name):
        source_url = os.environ["StorageUrl"] + self.blob.name
        destination_blob_name = self.get_destination_blob_name()

        self.blob_service.copy_blob(
            container_name=destination_container_name,
            blob_name=destination_blob_name,
            copy_source=source_url,
        )

    def get_destination_blob_name(self):
        blob_filename = self.blob.name.split("/")[1]
        datetime_str = datetime.today().strftime("%Y%m%d-%H%M%S")
        return f"{datetime_str}-{blob_filename}"

    def get_hesa_xml(self):
        storage_container_name = os.environ["AzureStorageAccountHesaContainerName"]
        storage_blob_name = os.environ["AzureStorageBlobName"]

        compressed_file = io.BytesIO()

        self.blob_service.get_blob_to_stream(storage_container_name, storage_blob_name, compressed_file, max_connections=1)

        compressed_file.seek(0)

        compressed_gzip = gzip.GzipFile(fileobj=compressed_file)

        decompressed_file = compressed_gzip.read()

        compressed_file.close()
        compressed_gzip.close()

        xml_string = decompressed_file.decode("utf-8")

        return xml_string
