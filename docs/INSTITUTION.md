### Institution Data

#### Cosmos db doc:

```json5
{
    "_id": "string",
    "created_at": "date",
    "version": "integer",
    "institution": {
        "apr_outcome": "string",
        "contact_details": {
            "address": {
                "line_1": "string",
                "line_2": "string",
                "line_3": "string",
                "town": "string",
                "county": "string",
                "post_code": "string"
            },
            "telephone": "string"
        },
        "courses": [
            {
                "distance_learning": {
                    "code": "string",
                    "label": "string"
                },
                "honours_award_provision": "boolean",
                "kis_course_id": "string",
                "kis_mode": {
                    "code": "string",
                    "label": "string"
                },
                "links": {
                    "course_page": {
                        "english": "string",
                        "welsh": "string"
                    },
                    "self": {
                        "english": "string",
                        "welsh": "string"
                    }
                },
                "locations": [
                    {
                        "name": {
                            "english": "string",
                            "welsh": "string"
                        }
                    }
                ],
                "qualification": {
                    "code": "string",
                    "label": "string"
                },
                "sandwich_year": {
                    "code": "string",
                    "label": "string"
                },
                "title": {
                    "english": "string",
                    "welsh": "string"
                },
                "year_abroad": {
                    "code": "string",
                    "label": "string"
                },
            }
            ...
        ],
        "links": {
            "institution_homepage": "string",
            "self": {
                "english": "string",
                "welsh": "string"
            },
            "student_union": [
                {
                    "english": "string",
                    "welsh": "string"
                }
            ]
        },
        "public_ukprn_name" : "string",
        "public_ukprn" : "string", // doc is unique based on the public_ukprn 
        "public_ukprn_country": {
            "code": "string",
            "label": "string"
        },
        "tef_outcome": "string",
        "total_number_of_courses": "integer",
        "ukprn_name" : "string",
        "ukprn" : "string",
        "ukprn_country": {
            "code": "string",
            "label": "string"
        }
    }
}
```

#### Mapping Xml fields to JSON fields

| JSON field name                                  | XML Path                                              | XML field name        |
|--------------------------------------------------|-------------------------------------------------------|-----------------------|
| institution.apr_outcome                          | KIS.INSTITUTION.APROutcome                            | APROutcome            |
| institution.contact_details.address.line_1       | Retreive data from UKPRN API                          | Address1              |
| institution.contact_details.address.line_2       | Retreive data from UKPRN API                          | Address2              |
| institution.contact_details.address.line_3       | Retreive data from UKPRN API                          | Address3              |
| institution.contact_details.address.town         | Retreive data from UKPRN API                          | Town                  |
| institution.contact_details.address.county       | Retreive data from UKPRN API                          | County                |
| institution.contact_details.address.post_code    | Retreive data from UKPRN API                          | PostCode              |
| institution.contact_details.telephone            | Retreive data from UKPRN API                          | ContactTelephone1     |
| institution.courses.[].distance_learning.code    | KIS.INSTITUTION.KISCOURSE.DISTANCE                    | DISTANCE              |
| institution.courses.[].distance_learning.label   | See Distance Learning Code values                     | N/A                   |
| institution.courses.[].honours_award_provision   | KIS.INSTITUTION.KISCOURSE.HONOURS                     | HONOURS               |
| institution.courses.[].kis_course_id             | KIS.INSTITUTION.KISCOURSE.KISCOURSEID                 | KISCOURSEID           |
| institution.courses.[].kis_mode.code             | KIS.INSTITUTION.KISCOURSE.KISMODE                     | KISMODE               |
| institution.courses.[].kis_mode.label            | See Mode Code values                                  | N/A                   |
| institution.courses.[].links.course_page.english | KIS.INSTITUTION.KISCOURSE.CRSEURL                     | CRSEURL               |
| institution.courses.[].links.course_page.welsh   | KIS.INSTITUTION.KISCOURSE.CRSEURLW                    | CRSEURLW              |
| institution.courses.[].links.self.english        | Dynamically built                                     | N/A                   |
| institution.courses.[].links.self.welsh          | Dynamically built                                     | N/A                   |
| institution.courses.[].locations.[].name.english | KIS.LOCATION.LOCNAME                                  | LOCNAME               |
| institution.courses.[].locations.[].name.welsh   | KIS.LOCATION.LOCNAMEW                                 | LOCNAMEW              |
| institution.courses.[].qualification.code        | KIS.INSTITUTION.KISCOURSE.KISAIMCODE                  | KISAIMCODE            |
| institution.courses.[].qualification.label       | KISAIM.KISAIMLABEL                                    | KISAIMLABEL           |
| institution.courses.[].sandwich_year.code        | KIS.INSTITUTION.KISCOURSE.SANDWICH                    | SANDWICH              |
| institution.courses.[].sandwich_year.label       | See Sandwich Years Code values                        | N/A                   |
| institution.courses.[].year_abroad.code          | KIS.INSTITUTION.KISCOURSE.YEARABROAD                  | YEARABROAD            |
| institution.courses.[].year_abroad.label         | See Year Abroad Code values                           | N/A                   |
| institution.courses.[].title.english             | KIS.INSTITUTION.KISCOURSE.TITLE                       | TITLE                 |
| institution.courses.[].title.welsh               | KIS.INSTITUTION.KISCOURSE.TITLEW                      | TITLEW                |
| institution.links.institution_homepage           | Retreive data from UKPRN API                          | ContactWebsiteAddress |
| institution.links.self.english                   | Dynamically built                                     | N/A                   |
| institution.links.self.welsh                     | Dynamically built                                     | N/A                   |
| institution.links.[].student_union.english       | LOCATION.SUURL                                        | SUURL                 |
| institution.links.[].student_union.welsh         | LOCATION.SUURLW                                       | SUURLW                |
| institution.public_ukprn_name                    | KIS.INSTITUTION.PUBUKPRN                              | PUBUKPRN              |
| institution.public_ukprn                         | Retreive data from UKPRN API                          | N/A                   |
| institution.public_ukprn_country.code            | KIS.INSTITUTION.PUBUKPRNCOUNTRY                       | PUBUKPRNCOUNTRY       |
| institution.public_ukprn_country.label           | See Country Code values                               | N/A                   |
| institution.tef_outcome                          | KIS.INSTITUTION.TEFOutcome                            | TEFOutcome            |
| institution.total_number_of_courses              | Count the list of courses in `institution.courses`    | N/A                   |
| institution.ukprn_name                           | KIS.INSTITUTION.UKPRN                                 | UKPRN                 |
| institution.ukprn                                | Retreive data from UKPRN API                          | N/A                   |
| institution.ukprn_country.code                   | KIS.INSTITUTION.COUNTRY                               | COUNTRY               |
| institution.ukprn_country.label                  | See Country Code values                               | N/A                   |


### Country Codes Values

| country.code value | country.name value |
|--------------------|--------------------|
| XF                 | England            |
| XG                 | Northern Ireland   |
| XH                 | Scotland           |
| XI                 | Wales              |

### Distance Learning Code Values

| distance_learning.code value | distance_learning.label value                            |
|------------------------------|----------------------------------------------------------|
| 0                            | Course is available other than by distance learning      |
| 1                            | Course is only available through distance learning       |
| 2                            | Course is optionally available through distance learning |

### Mode Code Values

| mode.code value | mode.label value |
|-----------------|------------------|
| 1               | Full-time        |
| 2               | Part-Time        |
| 3               | Both             |

### Sandwich Year Code Values

| sandwich_year.code value | sandwich_year.label value |
|--------------------------|---------------------------|
| 0                        | Not available             |
| 1                        | Optional                  |
| 2                        | Compulsory                |

### Year Abroad Code Values

| year_abroad.code value | year_abroad.label value |
|------------------------|-------------------------|
| 0                      | Not available           |
| 1                      | Optional                |
| 2                      | Compulsory              |