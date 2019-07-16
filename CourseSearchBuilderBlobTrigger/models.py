
def build_course_search_doc(course):
    json = {
        "@search.action": "upload",
        "id": course['id'],
        "course": {
            "country": {
                "code": course['course']['country']['code'],
                "label": course['course']['country']['label']
            },
            "distance_learning": {
                "code": course['course']['distance_learning']['code'],
                "label": course['course']['distance_learning']['label']
            },
            "foundation_year_availablity": {
                "code": course['course']['foundation_year_availablity']['code'],
                "label": course['course']['foundation_year_availablity']['label']
            },
            "honours_award_provision": course['course']['honours_award_provision'],
            "institution": {
                "pub_ukprn_name": course['course']['institution']['pub_ukprn_name'],
                "sort_pub_ukprn_name": course['course']['institution']['sort_pub_ukprn_name'],
                "pub_ukprn": course['course']['institution']['pub_ukprn']
            },
            "kis_course_id": course['course']['kis_course_id'],
            "length_of_course": {
                "code": course['course']['length_of_course']['code'],
                "label": course['course']['length_of_course']['label']
            },
            "locations": {
                "name": {
                    "english": course['course']['locations']['name']['english'],
                    "welsh": course['course']['locations']['name']['welsh']
                },
                "geo": {
                    "type": "Point",
                    "coordinates": [
                        course['course']['locations']['longitude'],
                        course['course']['locations']['latitude']
                    ]
                }
            },
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
            "title": {
                "english": course['course']['title']['english'],
                "welsh": course['course']['title']['welsh']
            },
            "year_abroad": {
                "code": course['course']['year_abroad']['code'],
                "label": course['course']['year_abroad']['label']
            }
        }
    }

    return json