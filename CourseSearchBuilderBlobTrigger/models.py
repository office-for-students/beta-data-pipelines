
def build_course_search_doc(course):
    sort_pub_ukprn_name = createSortableName(course['course']['institution']['pub_ukprn_name'])
    locations = buildLocations(course['course']['locations'])
    title = buildTitle(course['course']['title'])

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
            "length_of_course": {
                "code": course['course']['length_of_course']['code'],
                "label": course['course']['length_of_course']['label']
            },
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

def createSortableName(name):
    # TODO rip out logic from go search api for sortable name
    sortable_name = name
    return sortable_name

def buildLocations(locations):
    
    search_locations = []

    for location in locations:
        location_names = {}
        if 'english' in location['name']:
            location_names['english'] = location['name']['english']
        if 'welsh' in location['name']:
            location_names['welsh'] = location['name']['welsh']
        
        longitude = float(location['longitude'])
        latitude = float(location['latitude'])

        search_location = {
            "name": location_names,
            "geo": {
                "type": "Point",
                "coordinates": [
                    longitude,
                    latitude
                ]
            }
        }

        search_locations.append(search_location)

    return search_locations

def buildTitle(title):
    
    search_title = {}
    if 'english' in title:
        search_title['english'] = title['english']
    if 'welsh' in title:
        search_title['welsh'] = title['welsh']

    return search_title