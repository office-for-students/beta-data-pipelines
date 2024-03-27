import csv
import logging
from typing import Any
from typing import Dict

from services import exceptions


class QualificationCourseEnricher:
    """Handles enriching courses with Qualification data"""

    def __init__(self, csv_string: str = None) -> None:
        if csv_string:
            rows = csv_string.splitlines()

            # csv header row
            if not self.validate_column_headers(rows[0]):
                logging.error(
                    "file headers are incorrect, expecting the following: code, english_label, level, welsh_label"
                )
                raise exceptions.StopEtlPipelineErrorException

            self.qualification_levels = rows

    @staticmethod
    def validate_column_headers(header_row: str) -> bool:
        """
        Takes a header row string and ensures that the column headers are correct.
        Returns True if they are valid, otherwise False

        :param header_row: Header row to validate
        :type header_row: str
        :return: True if the header is valid, otherwise False
        :rtype: bool
        """
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
        """
        Takes a course and enriches ukprn names with UKRLP data.

        :param course: Course data
        :type course: Dict[str, Any]
        :return: None
        """

        qualification_code = course["course"]["qualification"]["code"]
        course["course"]["qualification"]["level"] = self.get_qualification_level(qualification_code)

    def get_qualification_level(self, code: str) -> str:
        """
        Takes a qualification code and returns the corresponding qualification found in the QualificationCourseEnricher
        object's CSV.

        :param code: Code used for lookup
        :type code: str
        :return: Qualification level string
        :rtype: str
        """
        rows = csv.reader(self.qualification_levels)
        for row in rows:
            if row[0] == code:
                return row[1]
        return ""
