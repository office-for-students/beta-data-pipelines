import os
import unittest

import dateutil

from datetime import datetime, timedelta, timezone
from unittest import mock
from dataset_creator import (
    DataSetCreator,
    get_time_in_minutes_since_given_datetime,
    convert_dt_str_to_dt_object,
    get_builds_value,
    get_initial_build_value,
)


class TestGetTimeInMinutesSinceGivenDatetime(unittest.TestCase):
    def test_get_time_in_minutes_since_given_datetime_with_60(self):
        dt_now = datetime.now(timezone.utc)
        dt_in_past = dt_now - timedelta(hours=1)
        time_in_minutes = get_time_in_minutes_since_given_datetime(dt_in_past)
        expected_time_in_minutes = 60
        self.assertEqual(time_in_minutes, expected_time_in_minutes)

    def test_get_time_in_minutes_since_given_datetime_with_110(self):
        dt_now = datetime.now(timezone.utc)
        dt_in_past = dt_now - timedelta(hours=1, minutes=50)
        time_in_minutes = get_time_in_minutes_since_given_datetime(dt_in_past)
        expected_time_in_minutes = 110
        self.assertEqual(time_in_minutes, expected_time_in_minutes)

    def test_get_time_in_minutes_since_given_datetime_with_120(self):
        dt_now = datetime.now(timezone.utc)
        dt_in_past = dt_now - timedelta(hours=2)
        time_in_minutes = get_time_in_minutes_since_given_datetime(dt_in_past)
        expected_time_in_minutes = 120
        self.assertEqual(time_in_minutes, expected_time_in_minutes)

    def test_get_time_in_minutes_since_given_datetime_with_one_day(self):
        dt_now = datetime.now(timezone.utc)
        dt_in_past = dt_now - timedelta(days=1)
        time_in_minutes = get_time_in_minutes_since_given_datetime(dt_in_past)
        expected_time_in_minutes = 24 * 60
        self.assertEqual(time_in_minutes, expected_time_in_minutes)

    def test_get_time_in_minutes_since_given_datetime_with_two_days(self):
        dt_now = datetime.now(timezone.utc)
        dt_in_past = dt_now - timedelta(days=2)
        time_in_minutes = get_time_in_minutes_since_given_datetime(dt_in_past)
        expected_time_in_minutes = 2 * 24 * 60
        self.assertEqual(time_in_minutes, expected_time_in_minutes)

    def test_get_time_in_minutes_since_given_datetime_with_seven_days(self):
        dt_now = datetime.now(timezone.utc)
        dt_in_past = dt_now - timedelta(days=7)
        time_in_minutes = get_time_in_minutes_since_given_datetime(dt_in_past)
        expected_time_in_minutes = 7 * 24 * 60
        self.assertEqual(time_in_minutes, expected_time_in_minutes)


class TestConvertDateStrToDtObject(unittest.TestCase):
    def test_with_isoformat_string(self):
        isoformat_dt_str = "2019-08-30T11:49:17.663837+00:00"
        dt_obj = convert_dt_str_to_dt_object(isoformat_dt_str)
        self.assertEqual(dt_obj.tzinfo, dateutil.tz.tz.tzutc())


class TestBuildSection(unittest.TestCase):
    def test_get_initial_build_value(self):
        initial_build_value = get_initial_build_value()
        self.assertEqual(initial_build_value, {"status": "pending"})

    def test_get_builds_value(self):
        builds_value = get_builds_value()
        expected_value = {}
        expected_value["courses"] = {"status": "pending"}
        expected_value["institutions"] = {"status": "pending"}
        expected_value["search"] = {"status": "pending"}
        self.assertEqual(builds_value, expected_value)


class TestHasEnoughTimeElapsedSinceLastDataSetCreated(unittest.TestCase):
    @mock.patch.dict(
        os.environ, {"TimeInMinsToWaitBeforeCreateNewDataSet": "120"}
    )
    @mock.patch("dataset_creator.get_cosmos_client")
    @mock.patch("dataset_creator.get_collection_link")
    def test_has_enough_time_elapsed_with_60(
        self, mock_cosmos_client, mock_get_collection_link
    ):
        dt_in_past = datetime.now(timezone.utc) - timedelta(hours=1)
        dsc = DataSetCreator()
        dsc.get_datetime_of_latest_dataset_doc = mock.MagicMock(
            return_value=dt_in_past
        )
        enough_time_elapsed = (
            dsc.has_enough_time_elaspsed_since_last_dataset_created()
        )
        self.assertFalse(enough_time_elapsed)

    @mock.patch.dict(
        os.environ, {"TimeInMinsToWaitBeforeCreateNewDataSet": "120"}
    )
    @mock.patch("dataset_creator.get_cosmos_client")
    @mock.patch("dataset_creator.get_collection_link")
    def test_has_enough_time_elapsed_with_120(
        self, mock_cosmos_client, mock_get_collection_link
    ):
        dt_in_past = datetime.now(timezone.utc) - timedelta(hours=2)
        dsc = DataSetCreator()
        dsc.get_datetime_of_latest_dataset_doc = mock.MagicMock(
            return_value=dt_in_past
        )
        enough_time_elapsed = (
            dsc.has_enough_time_elaspsed_since_last_dataset_created()
        )
        self.assertTrue(enough_time_elapsed)


# TODO add more tests
