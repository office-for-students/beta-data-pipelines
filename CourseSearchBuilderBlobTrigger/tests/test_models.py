import unittest
import os
import sys
import inspect
import json

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from models import build_course_search_doc, create_sortable_name,\
    build_locations, build_title, build_length_of_course


class TestCreateSortableName(unittest.TestCase):
    def test_lower_casing(self):
        input_name = 'BriStol University'
        expected_name = 'bristol university'

        name = create_sortable_name(input_name)
        self.assertEqual(expected_name, name)

    def test_replace_the_university_of(self):
        input_name = 'The University of Exeter'
        expected_name = 'exeter'

        name = create_sortable_name(input_name)
        self.assertEqual(expected_name, name)

    def test_replace_university_of(self):
        input_name = 'University of Bristol'
        expected_name = 'bristol'

        name = create_sortable_name(input_name)
        self.assertEqual(expected_name, name)


class TestBuildLocations(unittest.TestCase):
    def test_course_without_locations(self):
        input_course = {}
        expected_search_locations = []

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_empty_locations_object(self):
        input_course = {'locations': []}
        expected_search_locations = []

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_one_english_location_only(self):
        input_course = {'locations': [{
            'name': {'english': 'Bradford Campus'}
        }]}

        expected_search_locations = [{'name': {'english': 'Bradford Campus'}}]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_one_welsh_location_only(self):
        input_course = {'locations': [{
            'name': {'welsh': 'Prifysgol Caerdydd'}
        }]}

        expected_search_locations = [{'name': {'welsh': 'Prifysgol Caerdydd'}}]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_multiple_english_and_welsh_locations(self):
        input_course = {'locations': [{
            'name': {
                'english': 'Cardiff University',
                'welsh': 'Prifysgol Caerdydd'}
        }, {
            'name': {
                'english': 'Cardiff Campus',
                'welsh': 'Campus Caerdydd'}
        }]}

        expected_search_locations = [{
            'name': {
                'english': 'Cardiff University',
                'welsh': 'Prifysgol Caerdydd'}
        }, {
            'name': {
                'english': 'Cardiff Campus',
                'welsh': 'Campus Caerdydd'}
        }]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_geo_long_lats_only(self):
        input_course = {'locations': [{
            'latitude': '-1.45638',
            'longitude': '14.9431'
        }]}

        expected_search_locations = [{
            'geo': {'type': 'Point', 'coordinates': [14.9431, -1.45638]}
        }]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_only_long_only(self):
        input_course = {'locations': [{
            'longitude': '14.9431'
        }]}
        expected_search_locations = [{}]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_only_lat_only(self):
        input_course = {'locations': [{
            'latitude': '-1.45638'
        }]}
        expected_search_locations = [{}]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_multiple_geo_long_lats(self):
        input_course = {'locations': [{
            'latitude': '-1.45638', 'longitude': '14.9431'
        }, {
            'latitude': '-1.84578', 'longitude': '12.4536'
        }]}

        expected_search_locations = [{
            'geo': {'type': 'Point', 'coordinates': [14.9431, -1.45638]}
        }, {
            'geo': {'type': 'Point', 'coordinates': [12.4536, -1.84578]}
        }]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_single_full_fat_location(self):
        input_course = {'locations': [{
            'name': {
                'english': 'Cardiff University', 'welsh': 'Prifysgol Caerdydd'
            },
            'latitude': '-1.45638', 'longitude': '14.9431'
        }]}

        expected_search_locations = [{
            'name': {
                'english': 'Cardiff University', 'welsh': 'Prifysgol Caerdydd'
            },
            'geo': {'type': 'Point', 'coordinates': [14.9431, -1.45638]}
        }]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)

    def test_course_with_multiple_full_fat_locations(self):
        input_course = {'locations': [{
            'name': {
                'english': 'Cardiff University', 'welsh': 'Prifysgol Caerdydd'
            },
            'latitude': '-1.45638', 'longitude': '14.9431'
        }, {
            'name': {
                'english': 'Cardiff Campus', 'welsh': 'Campus Caerdydd'
            },
            'latitude': '-1.84578', 'longitude': '12.4536'
        }]}

        expected_search_locations = [{
            'name': {
                'english': 'Cardiff University', 'welsh': 'Prifysgol Caerdydd'
            },
            'geo': {'type': 'Point', 'coordinates': [14.9431, -1.45638]}
        }, {
            'name': {
                'english': 'Cardiff Campus', 'welsh': 'Campus Caerdydd'
            },
            'geo': {'type': 'Point', 'coordinates': [12.4536, -1.84578]}
        }]

        search_locations = build_locations(input_course)
        self.assertEqual(expected_search_locations, search_locations)


class TestBuildTitle(unittest.TestCase):
    def test_course_without_title_object(self):
        # kis_course_id, mode.code and institution.pub_ukprn are
        # needed to log warning for missing title
        input_course = {
            'kis_course_id': 'test_course_id',
            'mode': {'code': '1'},
            'institution': {'pub_ukprn': 'test_institution_id'}
        }
        expected_search_title = {}

        search_title = build_title(input_course)
        self.assertEqual(expected_search_title, search_title)

    def test_course_with_empty_title_object(self):
        input_course = {'title': {}}
        expected_search_title = {}

        search_title = build_title(input_course)
        self.assertEqual(expected_search_title, search_title)

    def test_course_with_english_title_only(self):
        input_course = {'title': {'english': 'mathematics'}}
        expected_search_title = {'english': 'mathematics'}

        search_title = build_title(input_course)
        self.assertEqual(expected_search_title, search_title)

    def test_course_with_welsh_title_only(self):
        input_course = {'title': {'welsh': 'mathemateg'}}
        expected_search_title = {'welsh': 'mathemateg'}

        search_title = build_title(input_course)
        self.assertEqual(expected_search_title, search_title)

    def test_course_with_english_and_welsh_title(self):
        input_course = {'title': {
            'english': 'mathematics', 'welsh': 'mathemateg'}
        }

        expected_search_title = {
            'english': 'mathematics', 'welsh': 'mathemateg'}

        search_title = build_title(input_course)
        self.assertEqual(expected_search_title, search_title)


class TestBuildLengthOfCourse(unittest.TestCase):
    def test_course_without_length_of_course(self):
        input_course = {}
        expected_search_length_of_course = {}

        search_length_of_course = build_length_of_course(input_course)
        self.assertEqual(expected_search_length_of_course,
                         search_length_of_course)

    def test_course_with_only_code_in_length_of_course(self):
        input_course = {'length_of_course': {'code': '1'}}
        expected_search_length_of_course = {}

        search_length_of_course = build_length_of_course(input_course)
        self.assertEqual(expected_search_length_of_course,
                         search_length_of_course)

    def test_course_with_only_label_in_length_of_course(self):
        input_course = {'length_of_course': {'label': '1st stage'}}
        expected_search_length_of_course = {}

        search_length_of_course = build_length_of_course(input_course)
        self.assertEqual(expected_search_length_of_course,
                         search_length_of_course)

    def test_course_with_valid_length_of_course(self):
        input_course = {'length_of_course': {
            'code': '1', 'label': '1st stage'}}

        expected_search_length_of_course = {'code': '1', 'label': '1st stage'}

        search_length_of_course = build_length_of_course(input_course)
        self.assertEqual(expected_search_length_of_course,
                         search_length_of_course)


def get_json(filename):
    """Reads json file in test dir into a string and return"""

    cwd = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(cwd, filename)) as json_file:
        jsn = json.load(json_file)
    return jsn


class TestBuildCourseSearchDoc(unittest.TestCase):
    def test_full_fat_course(self):
        input_course = get_json('fixtures/input-full-fat-course.json')
        expected_course = get_json('fixtures/output-full-fat-course.json')

        search_course_doc = build_course_search_doc(input_course)
        self.assertEqual(expected_course, search_course_doc)
