import logging


def build_course_search_doc(course):

    sort_pub_ukprn_name = create_sortable_name(
        course['course']['institution']['pub_ukprn_name']
    )

    locations = build_locations(course['course'])
    title = build_title(course['course'])
    length_of_course = build_length_of_course(course['course'])

    json = {
        "@search.action": "upload",
        "id": course['id'],
        "course": {
            "country": {
                "code": course['course']['country']['code'],
                "label": course['course']['country']['name']
            },
            "distance_learning": {
                "code": course['course']['distance_learning']['code'],
                "label": course['course']['distance_learning']['label']
            },
            "foundation_year_availability": {
                "code": course['course']['foundation_year_availability']['code'],
                "label": course['course']['foundation_year_availability']['label']
            },
            "honours_award_provision": course['course']['honours_award_provision'],
            "institution": {
                "pub_ukprn_name": course['course']['institution']['pub_ukprn_name'],
                "sort_pub_ukprn_name": sort_pub_ukprn_name,
                "pub_ukprn": course['course']['institution']['pub_ukprn']
            },
            "kis_course_id": course['course']['kis_course_id'],
            "length_of_course": length_of_course,
            "locations": locations,
            "mode": {
                "code": course['course']['mode']['code'],
                "label": course['course']['mode']['label']
            },
            "qualification": {
                "code": course['course']['qualification']['code'],
                "label": course['course']['qualification']['label']
            },
            "sandwich_year": {
                "code": course['course']['sandwich_year']['code'],
                "label": course['course']['sandwich_year']['label']
            },
            "title": title,
            "year_abroad": {
                "code": course['course']['year_abroad']['code'],
                "label": course['course']['year_abroad']['label']
            }
        }
    }

    return json


def create_sortable_name(name):

    # lowercase institution name
    sortable_name = name.lower()

    # remove unwanted prefixes
    sortable_name = sortable_name.replace('the university of ', '')
    sortable_name = sortable_name.replace('university of ', '')

    # remove unwanted commas
    sortable_name = sortable_name.replace(',', '')

    return sortable_name


def build_locations(course):

    search_locations = []
    if 'locations' in course:

        for location in course['locations']:
            search_location = {}

            if 'name' in location:
                location_names = {}

                if 'english' in location['name']:
                    location_names['english'] = location['name']['english']
                if 'welsh' in location['name']:
                    location_names['welsh'] = location['name']['welsh']

                search_location['name'] = location_names

            if 'longitude' in location and 'latitude' in location:
                longitude = float(location['longitude'])
                latitude = float(location['latitude'])

                search_location['geo'] = {
                    "type": "Point",
                    "coordinates": [
                        longitude,
                        latitude
                    ]
                }

            search_locations.append(search_location)

    return search_locations


def build_title(course):

    search_title = {}
    if "title" in course:
        if 'english' in course['title']:
            search_title['english'] = course['title']['english']
        if 'welsh' in course['title']:
            search_title['welsh'] = course['title']['welsh']
    else:
        logging.warning(f"course title missing\n course_id:{course['kis_course_id']}\n \
            course_mode: {course['mode']['code']}\n \
            institution_id: {course['institution']['pub_ukprn']}\n")

    return search_title


def build_length_of_course(course):

    search_length_of_course = {}
    if 'length_of_course' in course:

        if 'label' in course['length_of_course'] and \
           'code' in course['length_of_course']:

            search_length_of_course['code'] = course['length_of_course']['code']
            search_length_of_course['label'] = course['length_of_course']['label']

    return search_length_of_course
