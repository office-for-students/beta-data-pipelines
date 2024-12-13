import logging
import os
import csv
from constants import BLOB_HESA_BLOB_NAME
from constants import BLOB_HESA_CONTAINER_NAME
from constants import BLOB_SUBJECTS_BLOB_NAME
from constants import BLOB_SUBJECTS_CONTAINER_NAME
from constants import BLOB_TEST_BLOB_DIRECTORY
from constants import BLOB_WELSH_UNIS_BLOB_NAME
from constants import BLOB_WELSH_UNIS_CONTAINER_NAME
from constants import BLOB_QUALIFICATIONS_CONTAINER_NAME
from constants import BLOB_QUALIFICATIONS_BLOB_NAME
from services.blob_service.base import BlobServiceBase
from etl_pipeline.utils import clean_file_data
import gzip
import magic


class BlobServiceLocal(BlobServiceBase):
    """Blob service to be used when running locally (i.e. for testing purposes)"""

    def __init__(self, service_string: str = None):
        super().__init__()
        self.blob_path = BLOB_TEST_BLOB_DIRECTORY
        self.initialise_blob_directories()

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


        clean_files = [BLOB_HESA_BLOB_NAME, BLOB_QUALIFICATIONS_BLOB_NAME] # files that dont need to be cleaned

        file_path = f'{self.blob_path}{container_name}/{blob_name}.gz'
        print(f"Retrieving string file from {file_path}")

        file_type = magic.from_file(file_path, mime=True)
        print(f'FILE TYPE IS: {file_type}')

        # clean bad data
        if blob_name not in clean_files:
            clean_file_data(file_path, file_type)
            print("DATA CLEANED")

        file_lines = []
        if file_type == 'text/csv':
            try:
                with open(file_path, 'r', newline='') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        file_lines.append(','.join(row))
            except Exception as e:
                logging.error(f"An error occurred while reading the file: {e}")
                return ""
        else:
            try:
                # Open the GZIP file directly from local path
                with gzip.open(file_path, "rt", encoding="utf-8") as file:
                    reader = csv.reader(file)
                    for row in reader:
                        file_lines.append(','.join(row))
            except FileNotFoundError:
                logging.warning(f"File {file_path} not found")
                return ""
            except gzip.BadGzipFile:
                logging.error(f"File {file_path} not a gzip file or is corrupted")
                return ""
            except Exception as e:
                logging.error(f"An unexpected error occurred while reading the file: {e}")
                return ""


        # Format csv to match that of general blob service method
        for index, line in enumerate(file_lines):
            file_lines[index] = line.replace("\n", "")
        # file_lines = [line.replace("\n", "") for line in file_lines]
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
        file_path = self.blob_path + "/" + container_name + blob_name
        with open(file_path, "w") as file:
            file.write(encoded_file.decode("utf-8-sig"))

    def initialise_blob_directories(self) -> None:
        blob_directory = self.blob_path
        if not os.path.exists(blob_directory):
            os.makedirs(blob_directory)

        hesa_container_directory = self.blob_path + BLOB_HESA_CONTAINER_NAME
        subjects_container_directory = self.blob_path + BLOB_SUBJECTS_CONTAINER_NAME
        welsh_unis_container_directory = self.blob_path + BLOB_WELSH_UNIS_CONTAINER_NAME
        qualification_container_directory = self.blob_path + BLOB_QUALIFICATIONS_CONTAINER_NAME

        for directory in [hesa_container_directory, subjects_container_directory, welsh_unis_container_directory, qualification_container_directory]:
            if not os.path.exists(directory):
                logging.info(f"Blob directory {directory} does not exist. Creating...")
                os.makedirs(directory)
            else:
                logging.info(f"Blob directory {directory} found OK")

        hesa_blob = hesa_container_directory + "/" + BLOB_HESA_BLOB_NAME
        subjects_blob = subjects_container_directory + "/" + BLOB_SUBJECTS_BLOB_NAME
        welsh_unis_blob = welsh_unis_container_directory + "/" + BLOB_WELSH_UNIS_BLOB_NAME
        qualifications_blob = qualification_container_directory + "/" + BLOB_QUALIFICATIONS_BLOB_NAME

        if not os.path.exists(hesa_blob):
            logging.info(f"Blob {hesa_blob} does not exist. Creating...")
            with open(hesa_blob, "w") as file:
                file.write("<root> </root>")

        if not os.path.exists(subjects_blob):
            logging.info(f"Blob {subjects_blob} does not exist. Creating...")
            with open(subjects_blob, "w") as file:
                file.write("code,english_label,level,welsh_label")

        if not os.path.exists(welsh_unis_blob):
            logging.info(f"Blob {welsh_unis_blob} does not exist. Creating...")
            with open(welsh_unis_blob, "w") as file:
                file.write("ukprn,welsh_name")

        if not os.path.exists(qualifications_blob):
            logging.info(f"Blob {qualifications_blob} does not exist. Creating...")
            with open(qualifications_blob, "w") as file:
                file.write("code,level")
