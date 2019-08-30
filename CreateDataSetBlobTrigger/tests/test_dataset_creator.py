import unittest
from datetime import datetime, timedelta, timezone
from dataset_creator import get_time_in_minutes_since_given_datetime


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


# TODO add more tests
