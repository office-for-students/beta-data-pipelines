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
        clean_files = [BLOB_HESA_BLOB_NAME, BLOB_QUALIFICATIONS_BLOB_NAME]  # files that don't need to be cleaned

        # get the files
        blob_client = self.get_service_client().get_blob_client(container=container_name, blob=blob_name)
        response = blob_client.download_blob()
        file_content = io.BytesIO(response.readall())

        # check file type
        file_type = magic.from_buffer(file_content.getvalue(), mime=True)
        print(f'FILE TYPE IS: {file_type}')

        # only clean the problem files
        if blob_name not in clean_files:
            if file_type == 'application/gzip':
                self.clean_gzip_file(file_content)
            elif file_type == 'text/csv':
                self.clean_csv_file(file_content)
            print("DATA CLEANED")

        file_lines = []
        try:
            # open csv/text file and append to file lines
            if file_type == 'text/csv':
                file_content.seek(0)
                with io.TextIOWrapper(file_content, encoding='utf-8') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        file_lines.append(','.join(row))
            # open gzip file and append to file lines
            elif file_type == 'application/gzip':
                file_content.seek(0)
                with gzip.GzipFile(fileobj=file_content, mode='rt', encoding='utf-8') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        file_lines.append(','.join(row))
        except Exception as e:
            logging.error(f"An error occurred while reading the file: {e}")
            return ""

        file_string = "\n".join(line.replace("\n", "") for line in file_lines)
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

    def clean_csv_file(self, file_content: io.BytesIO) -> None:
        """
        Cleans the CSV data to remove unwanted BOM and excess whitespace.
        """
        cleaned_rows = []
        file_content.seek(0)
        reader = csv.reader(io.TextIOWrapper(file_content, encoding='utf-8'))
        for i, row in enumerate(reader):
            if i == 0 and row:
                row[0] = row[0].lstrip('\ufeff')
            cleaned_row = [field.strip() for field in row]
            cleaned_rows.append(cleaned_row)

        file_content.seek(0)
        file_content.truncate(0)
        writer = csv.writer(io.TextIOWrapper(file_content, encoding='utf-8', write_through=True), quoting=csv.QUOTE_ALL)
        writer.writerows(cleaned_rows)


    def clean_gzip_file(self, file_content: io.BytesIO) -> None:
        """
        Cleans the GZIP data to remove unwanted BOM and excess whitespace.
        """
        cleaned_rows = []
        file_content.seek(0)
        with gzip.GzipFile(fileobj=file_content, mode='rt', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            for i, row in enumerate(reader):
                if i == 0 and row:
                    row[0] = row[0].lstrip('\ufeff')
                cleaned_row = [field.strip() for field in row]
                cleaned_rows.append(cleaned_row)

        file_content.seek(0)
        file_content.truncate(0)
        with gzip.GzipFile(fileobj=file_content, mode='wt', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            writer.writerows(cleaned_rows)

