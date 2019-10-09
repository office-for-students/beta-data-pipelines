import os
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

    def create_blob_for_course_search_builder(self, version):
        destination_blob_name = f"dataset-complete-{version}"
        output_container_name = os.environ["CourseSearchBuilerContainerName"]

        self.blob_service.create_blob_from_text(
            container_name=output_container_name,
            blob_name=destination_blob_name,
            text=f'{{"version":{version}}}',
        )
