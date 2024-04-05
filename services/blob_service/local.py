from constants import BLOB_TEST_BLOB_DIRECTORY
from services.blob_service.base import BlobServiceBase


class BlobServiceLocal(BlobServiceBase):
    """Blob service to be used when running locally (i.e. for testing purposes)"""

    def __init__(self, service_string: str = None):
        super().__init__()
        self.blob_path = BLOB_TEST_BLOB_DIRECTORY

    def get_service_client(self) -> None:
        return None

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
        print(f"Retrieving string file from {file_path}")
        with open(file_path, "r") as file:
            file_lines = file.readlines()

        # Format csv to match that of general blob service method
        for index, line in enumerate(file_lines):
            file_lines[index] = line.replace("\n", "")

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

