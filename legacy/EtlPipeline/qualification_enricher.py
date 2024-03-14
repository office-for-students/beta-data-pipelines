import csv
import logging
from typing import Any
from typing import Dict

from legacy.services import exceptions
from legacy.services.blob import BlobService


class QualificationCourseEnricher:
    """Handles enriching courses with Qualification data"""

    def __init__(self, storage_container_name: str, storage_blob_name: str) -> None:
        blob_service = BlobService()

        csv_string = blob_service.get_str_file(storage_container_name, storage_blob_name)

        if csv_string:
            rows = csv_string.splitlines()

            # csv header row
            if not self.validate_column_headers(rows[0]):
                logging.error(
                    "file headers are incorrect, expecting the following: code, english_label, level, welsh_label"
                )
                raise exceptions.StopEtlPipelineErrorException

            self.qualification_levels = rows

    def validate_column_headers(self, header_row: str) -> bool:
        logging.info(f"Validating header row, headers: {header_row}")
        header_list = header_row.split(",")

        try:
            valid = True
            if header_list[0] != "code":
                logging.info(f"got in code: {header_list[0]}")
                valid = False

            if header_list[1] != "level":
                logging.info(f"got in level: {header_list[1]}")
                valid = False
        except IndexError:
            logging.exception(f"index out of range\nheader_row:{header_row}")
            valid = False

        return valid

    def enrich_course(self, course: Dict[str, Any]) -> None:
        """Takes a course and enriches ukprn names with UKRLP data"""

        qualification_code = course["course"]["qualification"]["code"]
        course["course"]["qualification"]["level"] = self.get_qualification_level(qualification_code)

    def get_qualification_level(self, code: str) -> str:
        rows = csv.reader(self.qualification_levels)
        for row in rows:
            if row[0] == code:
                return row[1]
        return ""
