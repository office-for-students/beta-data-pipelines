import unittest
from unittest import mock
import ukrlp_enricher

TEST_LOOKUP = {
    '10002863': {
        "id": "d1bf0a06-9cae-11e9-b1d0-8c859021ae2e",
        "created_at": "2019-07-02T09:50:26.871179",
        "ukprn": "10002863",
        "ukprn_name": "ACME TESTING COLLEGE",
        "contact_details": {
            "address": {
                "line_1": "Flat 1",
                "line_2": "No. 10",
                "line_3": "The High Street",
                "line_4": "Northend",
                "town": "Little Whingeing",
                "county": "Shire",
                "post_code": "CB1 1AA"
            },
            "telephone": "01632 960661"
        },
    },
    '10002718': {
        "id": "cf1dce22-9cae-11e9-af84-8c859021ae2e",
        "created_at": "2019-07-02T09:50:22.459071",
        "ukprn": "10002718",
        "ukprn_name": "GOLDSMITHS' COLLEGE, UNIVERSITY OF LONDON",
        "contact_details": {
            "address": {
                "line_1": "Flat 1",
                "line_2": "No. 10",
                "line_3": "The High Street",
                "line_4": "Northend",
                "town": "Little Whingeing",
                "county": "Shire",
                "post_code": "CB1 1AA"
            },
            "telephone": "01632 960661"
        },
    }
}


EXPECTED_COURSE = {
    "id": "1d2fdd92-9cbb-11e9-b644-8c859021ae2e",
    "created_at": "2019-07-02T11:18:27.400468",
    "version": "1",
    "course": {
        "application_provider": "10004930",
        "country": {
            "code": "XF",
            "name": "England"
        },
        "distance_learning": {
            "code": "0",
            "label": "Course is availble other than by distance learning"
        },
        "foundation_year_availability": {
            "code": "0",
            "label": "Not available"
        },
        "honours_award_provision": "0",
        "institution": {
            "pub_ukprn_name": "ACME TESTING COLLEGE",
            "pub_ukprn": "10002863",
            "ukprn_name": "ACME TESTING COLLEGE",
            "ukprn": "10002863",
        },
        "kis_course_id": "AB20",
        "length_of_course": {
            "code": "2",
            "label": "2 stages"
        },
        "links": {
            "accomodation": [
                {
                    "english": "http://www.brookes.ac.uk/studying-at-brookes/accommodation/halls-in-detail/"
                }
            ],
            "assesment_method": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "course_page": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "employment_details": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "financial_support_details": {
                "english": "http://www.brookes.ac.uk/studying-at-brookes/finance/undergraduate-finance---uk-and-eu-students/financial-support/financial-support-uk-eu-2018-19/"
            },
            "learning_and_teaching_methods": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "student_union": {
                "english": "http://www.nus.org.uk/en/students-unions/abingdon-and-witney-college-students-union/"
            }
        },
        "locations": [
            {
                "latitude": "51.8202",
                "longitude": "-1.477227",
                "name": {
                    "english": "Abingdon &amp; Witney College (Common Leys Campus)"
                }
            }
        ],
        "mode": {
            "code": "1",
            "label": "Full-time"
        },
        "qualification": {
            "code": "036",
            "label": "FdSc"
        },
        "sandwich_year": {
            "code": "0",
            "label": "Not available"
        },
        "title": {
            "english": "Animal Behaviour and Welfare"
        },
        "year_abroad": {
            "code": "0",
            "label": "Not available"
        }
    },
}

test_course = {
    "id": "1d2fdd92-9cbb-11e9-b644-8c859021ae2e",
    "created_at": "2019-07-02T11:18:27.400468",
    "version": "1",
    "course": {
        "application_provider": "10004930",
        "country": {
            "code": "XF",
            "name": "England"
        },
        "distance_learning": {
            "code": "0",
            "label": "Course is availble other than by distance learning"
        },
        "foundation_year_availability": {
            "code": "0",
            "label": "Not available"
        },
        "honours_award_provision": "0",
        "institution": {
            "pub_ukprn_name": "n/a",
            "pub_ukprn": "10002863",
            "ukprn_name": "n/a",
            "ukprn": "10002863",
        },
        "kis_course_id": "AB20",
        "length_of_course": {
            "code": "2",
            "label": "2 stages"
        },
        "links": {
            "accomodation": [
                {
                    "english": "http://www.brookes.ac.uk/studying-at-brookes/accommodation/halls-in-detail/"
                }
            ],
            "assesment_method": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "course_page": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "employment_details": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "financial_support_details": {
                "english": "http://www.brookes.ac.uk/studying-at-brookes/finance/undergraduate-finance---uk-and-eu-students/financial-support/financial-support-uk-eu-2018-19/"
            },
            "learning_and_teaching_methods": {
                "english": "http://www.abingdon-witney.ac.uk/course/?code=EHWE103P&year=A18/19&title=Animal+Behaviour+and+Welfare%2C+Foundation+Degree+"
            },
            "student_union": {
                "english": "http://www.nus.org.uk/en/students-unions/abingdon-and-witney-college-students-union/"
            }
        },
        "locations": [
            {
                "latitude": "51.8202",
                "longitude": "-1.477227",
                "name": {
                    "english": "Abingdon &amp; Witney College (Common Leys Campus)"
                }
            }
        ],
        "mode": {
            "code": "1",
            "label": "Full-time"
        },
        "qualification": {
            "code": "036",
            "label": "FdSc"
        },
        "sandwich_year": {
            "code": "0",
            "label": "Not available"
        },
        "title": {
            "english": "Animal Behaviour and Welfare"
        },
        "year_abroad": {
            "code": "0",
            "label": "Not available"
        }
    },
}

class TestEnricher(unittest.TestCase):

    @mock.patch('ukrlp_enricher.utils')
    def test_enrich_course(self, mock_utils):
        """Test a course is updated correctly with lookup data"""

        mock_utils.get_ukrlp_lookups.return_value = TEST_LOOKUP
        ukrlp_course_enricher = ukrlp_enricher.UkRlpCourseEnricher()
        ukrlp_course_enricher.enrich_course(test_course)
        self.assertDictEqual(test_course, EXPECTED_COURSE)

# TODO add more tests - including test when no lookup available for course

if __name__ == '__main__':
    unittest.main()
