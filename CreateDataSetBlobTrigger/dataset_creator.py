import inspect
import logging
import os
import sys

from datetime import datetime, timezone
from dateutil import parser

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

# from SharedCode.utils import get_cosmos_client, get_collection_link
from SharedCode.dataset_helper import DataSetHelper

from SharedCode.exceptions import DataSetTooEarlyError


class DataSetCreator:
    def __init__(self):
        self.dsh = DataSetHelper()

    def load_new_dataset_doc(self):
        dataset_doc = self.get_next_dataset_doc()
        if dataset_doc["version"] != 1:
            if not self.has_enough_time_elaspsed_since_last_dataset_created():
                raise DataSetTooEarlyError
        self.dsh.create_item(dataset_doc)
        logging.info(f"Created new vertsion {dataset_doc['version']} DataSet")

    def get_next_dataset_doc(self):
        next_version_number = self.get_next_dataset_version_number()
        dataset_doc = {}
        dataset_doc["builds"] = get_builds_value()
        dataset_doc["created_at"] = datetime.now(timezone.utc).isoformat()
        dataset_doc["is_published"] = False
        dataset_doc["status"] = "in progress"
        dataset_doc["version"] = next_version_number
        return dataset_doc

    def get_next_dataset_version_number(self):
        if self.get_number_of_dataset_docs() == 0:
            return 1
        return self.dsh.get_latest_version_number() + 1

    def get_number_of_dataset_docs(self):
        query = "SELECT * FROM c "
        data_set_list = self.dsh.query_items(query)
        return len(data_set_list)

    def has_enough_time_elaspsed_since_last_dataset_created(self):
        dt_of_latest_dataset_doc = self.get_datetime_of_latest_dataset_doc()
        time_in_minutes_since_latest_dataset_doc = get_time_in_minutes_since_given_datetime(
            dt_of_latest_dataset_doc
        )
        time_in_minutes_to_wait = int(
            os.environ["TimeInMinsToWaitBeforeCreateNewDataSet"]
        )
        if time_in_minutes_to_wait > time_in_minutes_since_latest_dataset_doc:
            return False
        return True

    def get_datetime_of_latest_dataset_doc(self):
        max_version_number = self.dsh.get_latest_version_number()
        query = f"SELECT * FROM c WHERE c.version = {max_version_number}"
        latest_doc = self.dsh.query_items(query)[0]
        return convert_dt_str_to_dt_object(latest_doc["created_at"])


def get_time_in_minutes_since_given_datetime(dt_in_the_past):
    """ Get time diff from now in minutes for timezone aware dt passed in"""
    dt_now = datetime.now(timezone.utc)
    return round((dt_now - dt_in_the_past).total_seconds() / 60)


def convert_dt_str_to_dt_object(dt_str):
    return parser.isoparse(dt_str)


def get_builds_value():
    builds = {}
    builds["courses"] = get_initial_build_value()
    builds["institutions"] = get_initial_build_value()
    builds["search"] = get_initial_build_value()
    return builds


def get_initial_build_value():
    return {"status": "pending"}
