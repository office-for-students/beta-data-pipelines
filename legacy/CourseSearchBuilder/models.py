import logging
from typing import Any
from typing import Dict
from typing import List


def build_course_search_doc(course: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes course data and returns a JSON dictionary with locations, course title, course length, and subjects appended.

    :param course: Course data as a dictionary
    :type course: Dict[str, Any]
    :return: JSON dictionary with extra course data
    :rtype: Dict[str, Any]
    """
    try:
        sort_pub_ukprn_name = create_sortable_name(
            course["course"]["institution"]["pub_ukprn_name"]
        )
        sort_pub_ukprn_welsh_name = create_sortable_name(
            course["course"]["institution"]["pub_ukprn_welsh_name"]
        )

        locations = build_locations(course["course"])
        title = build_title(course["course"])
        length_of_course = build_length_of_course(course["course"])
        subjects = build_subjects(course["course"])

        json = {
            "@search.action": "upload",
            "id": course["id"],
            "course": {
                "country": {
                    "code": course["course"]["country"]["code"],
                    "label": course["course"]["country"]["name"],
                },
                "distance_learning": {
                    "code": course["course"]["distance_learning"]["code"],
                    "label": course["course"]["distance_learning"]["label"],
                },
                "foundation_year_availability": {
                    "code": course["course"]["foundation_year_availability"][
                        "code"
                    ],
                    "label": course["course"]["foundation_year_availability"][
                        "label"
                    ],
                },
                "honours_award_provision": course["course"][
                    "honours_award_provision"
                ],
                "institution": {
                    "pub_ukprn_name": course["course"]["institution"][
                        "pub_ukprn_name"
                    ],
                    "pub_ukprn_welsh_name": course["course"]["institution"][
                        "pub_ukprn_welsh_name"
                    ],
                    "sort_pub_ukprn_name": sort_pub_ukprn_name,
                    "sort_pub_ukprn_welsh_name": sort_pub_ukprn_welsh_name,
                    "pub_ukprn": course["course"]["institution"]["pub_ukprn"],
                },
                "kis_course_id": course["course"]["kis_course_id"],
                "length_of_course": length_of_course,
                "locations": locations,
                "mode": {
                    "code": course["course"]["mode"]["code"],
                    "label": course["course"]["mode"]["label"],
                },
                "qualification": {
                    "code": course["course"]["qualification"]["code"],
                    "label": course["course"]["qualification"]["label"],
                    "level": course["course"]["qualification"]["level"],
                },
                "sandwich_year": {
                    "code": course["course"]["sandwich_year"]["code"],
                    "label": course["course"]["sandwich_year"]["label"],
                },
                "subjects": subjects,
                "title": title,
                "year_abroad": {
                    "code": course["course"]["year_abroad"]["code"],
                    "label": course["course"]["year_abroad"]["label"],
                },
            },
        }

        return json

    except Exception:
        raise


def create_sortable_name(name: str) -> str:
    """
    Takes an institution name and removes prefixes and commas, allowing it to be sorted. Returns sortable
    institution name as a lowercase string.

    :param name: Name of institution to create sortable name from
    :type name: str
    :return: Lowercase, sortable institution name
    :rtype: str
    """
    # lowercase institution name
    sortable_name = name.lower()

    # remove unwanted prefixes
    sortable_name = sortable_name.replace("the university of ", "")
    sortable_name = sortable_name.replace("university of ", "")
    sortable_name = sortable_name.replace("the ", "")
    sortable_name = sortable_name.replace("prifysgol ", "")
    sortable_name = sortable_name.replace("coleg ", "")
    sortable_name = sortable_name.replace("y coleg ", "")
    sortable_name = sortable_name.replace("brifysgol ", "")

    # remove unwanted commas
    sortable_name = sortable_name.replace(",", "")

    return sortable_name


def build_locations(course: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Takes a course as a dictionary and builds a list of locations from the course, and returns the search location data.

    :param course: Course data to build list of locations from
    :type course: Dict[str, Any]
    :return: List of location data
    :rtype: List[Dict[str, Any]]
    """
    search_locations = []
    if "locations" in course:

        for location in course["locations"]:
            search_location = {}

            if "name" in location:
                location_names = {}

                if "english" in location["name"]:
                    location_names["english"] = location["name"]["english"]
                if "welsh" in location["name"]:
                    location_names["welsh"] = location["name"]["welsh"]

                search_location["name"] = location_names

            if "longitude" in location and "latitude" in location:
                longitude = float(location["longitude"])
                latitude = float(location["latitude"])

                search_location["geo"] = {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                }

            search_locations.append(search_location)

    return search_locations


def build_subjects(course: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Takes course data and build a list of subjects from the course, and returns subject data.

    :param course: Course data to build list of subjects from
    :type course: Dict[str, Any]
    :return: List of subjects of course
    :rtype: List[Dict[str, Any]]
    """
    subjects = []
    if "subjects" in course:
        subjects = course["subjects"]

    return subjects


def build_title(course: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts and returns title data from the passed course

    :param course: Course data to create title data from
    :type course: Dict[str, Any]
    :return: Title data
    :rtype: Dict[str, str]
    """
    search_title = {}
    if "title" in course:
        if "english" in course["title"]:
            search_title["english"] = course["title"]["english"]
        if "welsh" in course["title"]:
            search_title["welsh"] = course["title"]["welsh"]
    else:
        logging.warning(
            f"course title missing\n course_id:{course['kis_course_id']}\n \
            course_mode: {course['mode']['code']}\n \
            institution_id: {course['institution']['pub_ukprn']}\n"
        )

    return search_title


def build_length_of_course(course: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts and returns course length data from the passed course

    :param course: Course data to create course length data from
    :type course: Dict[str, Any]
    :return: Course length data
    :rtype: Dict[str, Any]
    """
    try:
        search_length_of_course = {}
        if "length_of_course" in course:

            if (
                    "label" in course["length_of_course"]
                    and "code" in course["length_of_course"]
            ):
                code = str(course["length_of_course"]["code"])
                search_length_of_course["code"] = code
                search_length_of_course["label"] = course["length_of_course"]["label"]

        return search_length_of_course

    except Exception:
        raise
