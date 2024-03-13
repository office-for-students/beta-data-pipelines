import inspect
import logging
import os
import sys
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Dict

from dateutil import parser
from decouple import config

# from SharedCode.utils import get_cosmos_client, get_collection_link
from legacy.services.dataset_service import DataSetService
from legacy.services.exceptions import DataSetTooEarlyError

CURRENT_DIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)
sys.path.insert(0, PARENT_DIR)


class DataSetCreator:
    def __init__(self, test_mode=False) -> None:
        self.dsh = DataSetService()
        self.test_mode = test_mode

    def load_new_dataset_doc(self) -> None:
        dataset_doc = self.get_next_dataset_doc()

        if not self.test_mode:
            if dataset_doc["version"] != 1:
                if not self.has_enough_time_elaspsed_since_last_dataset_created():
                    raise DataSetTooEarlyError

        self.dsh.create_item(dataset_doc)
        logging.info(f"Created new version {dataset_doc['version']} DataSet")

    def get_next_dataset_doc(self) -> Dict[str, Any]:
        next_version_number = self.get_next_dataset_version_number()
        dataset_doc = {
            "builds": get_builds_value(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "is_published": False,
            "status": "in progress",
            "version": next_version_number,
        }
        return dataset_doc

    def get_next_dataset_version_number(self) -> int:
        if self.get_number_of_dataset_docs() == 0:
            return 1

        version = int(self.dsh.get_latest_version_number()) + 1
        return version
        # return self.dsh.get_latest_version_number() + 1

    def get_number_of_dataset_docs(self) -> int:
        query = "SELECT * FROM c "
        data_set_list = self.dsh.query_items(query)
        return len(data_set_list)

    def has_enough_time_elaspsed_since_last_dataset_created(self) -> bool:
        dt_of_latest_dataset_doc = self.get_datetime_of_latest_dataset_doc()
        time_in_minutes_since_latest_dataset_doc = get_time_in_minutes_since_given_datetime(
            dt_of_latest_dataset_doc
        )
        time_in_minutes_to_wait = int(
            config("MINUTES_WAIT_BEFORE_CREATE_NEW_DATASET")
        )
        if time_in_minutes_to_wait > time_in_minutes_since_latest_dataset_doc:
            return False
        return True

    def get_datetime_of_latest_dataset_doc(self) -> datetime:
        max_version_number = self.dsh.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {max_version_number}"
        latest_doc = self.dsh.query_items(query)[0]
        return convert_dt_str_to_dt_object(latest_doc["created_at"])


def get_time_in_minutes_since_given_datetime(dt_in_the_past: datetime) -> int:
    """ Get time diff from now in minutes for timezone aware dt passed in"""
    dt_now = datetime.now(timezone.utc)
    return round((dt_now - dt_in_the_past).total_seconds() / 60)


def convert_dt_str_to_dt_object(dt_str: str) -> datetime:
    return parser.isoparse(dt_str)


def get_builds_value() -> Dict[str, Dict[str, str]]:
    builds = {
        "courses": get_initial_build_value(),
        "institutions": get_initial_build_value(),
        "search": get_initial_build_value(),
        "subjects": get_initial_build_value()
    }
    return builds


def get_initial_build_value() -> Dict[str, str]:
    return {"status": "pending"}
