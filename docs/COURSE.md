### Course Data

#### Cosmos db doc:

```json5
{
    "_id": "string",
    "created_at": "date",
    "version": "integer",
    "course": {
        "accreditation": [
            {
                "accreditor_url": "string",
                "dependent_on": {
                    "code": "string",
                    "label": "string"
                },
                "text": {
                    "english": "string",
                    "welsh": "string"
                },
                "type": "string",
                "url": {
                    "english": "string",
                    "welsh": "string"
                }
            }
        ],
        "application_provider": "string",
        "country": {
            "code": "string",
            "name": "string"
        },
        "distance_learning": {
            "code": "integer",
            "label": "string"
        },
        "foundation_year_availability": {
            "code": "integer",
            "label": "string"
        },
        "honours_award_provision": "boolean",
        "institution": {
            "public_ukprn_name": "string",
            "public_ukprn": "string",
            "ukprn_name": "string",
            "ukprn": "string"
        },
        "kis_course_id": "string",
        "length_of_course": {
            "code": "integer",
            "label": "string"
        },
        "links": {
            "accommodation": {
                "english": "string",
                "welsh": "string"
            },
            "assessment_method": {
                "english": "string",
                "welsh": "string"
            },
            "course_page": {
                "english": "string",
                "welsh": "string"
            },
            "employment_details": {
                "english": "string",
                "welsh": "string"
            },
            "financial_support_details": {
                "english": "string",
                "welsh": "string"
            },
            "institution": "string",
            "learning_and_teaching_methods": {
                "english": "string",
                "welsh": "string"
            },
            "self": "string",
            "student_union": {
                "english": "string",
                "welsh": "string"
            }
        },
        "location_changes": "boolean",
        "locations": [
            {
                "latitude": "string",
                "longitude": "string",
                "name": {
                    "english": "string",
                    "welsh": "string"
                }
            }
        ],
        "mode": {
            "code": "integer",
            "label": "string"
        },
        "nhs_funded": {
            "code": "integer",
            "label": "string"
        },
        "qualification": {
            "code": "string",
            "label": "string"
        },
        "sandwich_year": {
            "code": "string",
            "label": "string"
        },
        "statistics": {
            ...See STATISTICS.md
        },
        "title": {
            "english": "string",
            "welsh": "string"
        },
        "ucas": {
            "course_id": "string",
            "programme_id": "string"
        },
        "year_abroad": {
            "code": "integer",
            "label": "Not available"
        }
    }
}
```

#### Mapping Xml fields to JSON fields

| JSON field name                            | XML Path                                              | XML field name   |
|--------------------------------------------|-------------------------------------------------------|------------------|
| accreditation.[].accreditor_url            | KIS.ACCREDITATIONTABLE.ACCURL                         | ACCURL           |
| accreditation.[].dependent_on.code         | KIS.ACCREDITATIONTABLE.ACCDEPEND                      | ACCDEPEND        |
| accreditation.[].dependent_on.label        | See Accreditation Code values                         | N/A              |
| accreditation.[].text.english              | KIS.ACCREDITATIONTABLE.ACCTEXT                        | ACCTEXT          |
| accreditation.[].text.welsh                | KIS.ACCREDITATIONTABLE.ACCTEXTW                       | ACCTEXTW         |
| accreditation.[].type                      | KIS.INSTITUTIOM.KISCOURSE.ACCTYPE                     | ACCTYPE          |
| accreditation.[].url.english               | KIS.INSTITUTIOM.KISCOURSE.ACCDEPENDURL                | ACCDEPENDURL     |
| accreditation.[].url.welsh                 | KIS.INSTITUTIOM.KISCOURSE.ACCDEPENDURLW               | ACCDEPENDURLW    |
| application_provider                       | KIS.INSTITUTION.KISCOURSE.UKPRNAPPLY                  | UKPRNAPPLY       |
| country.code                               | KIS.INSTITUTION.COUNTRY                               | COUNTRY          |
| country.name                               | See Country Code values                               | N/A              |
| distance_learning.code                     | KIS.INSTITUTION.KISCOURSE.DISTANCE                    | DISTANCE         |
| distance_learning.label                    | See Distance Learning Code values                     | N/A              |
| foundation_year_availability.code          | KIS.INSTITUTION.KISCOURSE.FOUNDATION                  | FOUNDATION       |
| foundation_year_availability.label         | See Foundation Year Availability Code values          | N/A              |
| honours_award_provision                    | KIS.INSTITUTION.KISCOURSE.HONOURS                     | HONOURS          |
| institution.public_ukprn_name              | Retreive data from UKPRN API                          | N/A              |
| institution.public_ukprn                   | KIS.INSTITUTION.PUBUKPRN                              | PUBUKPRN         |
| institution.ukprn_name                     | Retreive data from UKPRN API                          | N/A              |
| institution.ukprn                          | KIS.INSTITUTION.UKPRN                                 | UKPRN            |
| kis_course_id                              | KIS.INSTITUTIOM.KISCOURSE.KISCOURSEID                 | KISCOURSEID      |
| length_of_course.code                      | KIS.INSTITUTIOM.KISCOURSE.NUMSTAGE                    | NUMSTAGE         |
| length_of_course.label                     | See Length of Course Code values                      | N/A              |
| links.accommodation.english                | KIS.LOCATION.ACCOMURL                                 | ACCOMURL         |
| links.accommodation.welsh                  | KIS.LOCATION.ACCOMURLW                                | ACCOMURLW        |
| links.assessment_method.english            | KIS.INSTITUTION.KISCOURSE.ASSURL                      | ASSURL           |
| links.assessment_method.welsh              | KIS.INSTITUTION.KISCOURSE.ASSMURLW                    | ASSURLW          |
| links.course_page.english                  | KIS.INSTITUTION.KISCOURSE.CRSEURL                     | CRSEURL          |
| links.course_page.welsh                    | KIS.INSTITUTION.KISCOURSE.CRSEURLW                    | CRSEURLW         |
| links.employment.english                   | KIS.INSTITUTION.KISCOURSE.EMPLOYURL                   | EMPLOYURL        |
| links.employment.welsh                     | KIS.INSTITUTION.KISCOURSE.EMPLOYURLW                  | EMPLOYURLW       |
| links.financial_support.english            | KIS.INSTITUTION.KISCOURSE.SUPPORTURL                  | SUPPORTURL       |
| links.financial_support.welsh              | KIS.INSTITUTION.KISCOURSE.SUPPORTURLW                 | SUPPORTURLW      |
| links.institution.english                  | Dynamically built                                     | N/A              |
| links.institution.welsh                    | Dynamically built                                     | N/A              |
| links.learning_and_teaching_method.english | KIS.INSTITUTION.KISCOURSE.LTURL                       | LTURL            |
| links.learning_and_teaching_method.welsh   | KIS.INSTITUTION.KISCOURSE.LTURLW                      | LTURLW           |
| links.self.english                         | Dynamically built                                     | N/A              |
| links.self.welsh                           | Dynamically built                                     | N/A              |
| links.student_union.english                | KIS.INSTITUTION.SUURL                                 | SUURL            |
| links.student_union.welsh                  | KIS.INSTITUTION.SUURLW                                | SUURLW           |
| location_changes                           | KIS.INSTITUTION.KISCOURSE.LOCCHANGE                   | LOCCHANGE        |
| locations.[].latitude                      | KIS.LOCATION.LATITUDE                                 | LATITUDE         |
| locations.[].longitude                     | KIS.LOCATION.LONGITUDE                                | LONGITUDE        |
| locations.[].name.english                  | KIS.LOCATION.LOCNAME                                  | LOCNAME          |
| locations.[].name.welsh                    | KIS.LOCATION.LOCNAMEW                                 | LOCNAMEW         |
| mode.code                                  | KIS.INSTITUTION.KISCOURSE.KISMODE                     | KISMODE          |
| mode.label                                 | See Mode Code values                                  | N/A              |
| nhs_funded.code                            | KIS.INSTITUTION.KISCOURSE.NHS                         | NHS              |
| nhs_funded.label                           | See NHS Code values                                   | N/A              |
| qualification.code                         | KIS.INSTITUTION.KISCOURSE.KISAIMCODE                  | KISAIMCODE       |
| qualification.label                        | KIS.KISAIM.KISAIMLABEL                                | KISAIMLABEL      |
| sandwich_year.code                         | KIS.INSTITUTION.KISCOURSE.SANDWICH                    | SANDWICH         |
| sandwich_year.label                        | See Sandwich Years Code values                        | N/A              |
| statistics.*                               | See [STATISTICS](STATISTICS.md)                       | N/A              |
| title.english                              | KIS.INSTITUTION.KISCOURSE.TITLE                       | TITLE            |
| title.welsh                                | KIS.INSTITUTION.KISCOURSE.TITLEW                      | TITLEW           |
| ucas.course_id                             | KIS.INSTITUTION.KISCOURSE.COURSELOCATION.UCASCOURSEID | UCASCOURSEID     |
| ucas.programme_id                          | KIS.INSTITUTION.KISCOURSE.UCASPROGID                  | UCASPROGID       |
| year_abroad.code                           | KIS.INSTITUTION.KISCOURSE.YEARABROAD                  | YEARABROAD       |
| year_abroad.label                          | See Year Abroad Code values                           | N/A              |

### Notes: 

#### Accreditation

Use the `ACCTYPE` value to match fields accross `KISCOURSE.ACCREDITATION` and `ACCREDITATIONTABLE`.

#### Location

Use the `COURSELOCATION.LOCID` and `UKPRN` values to match fields accross `KISCOURSE` to `LOCATION`

#### Qualification

Use the `KISAIMCODE` value to find `KISAIMLABEL` in the `KISAIM` section

### Accreditation Code Values

| accreditation.[].dependent_on.code value | accreditation.[].dependent_on.label value        |
|------------------------------------------|--------------------------------------------------|
| 0                                        | Accreditation is not dependent on student choice |
| 1                                        | Accreditation is dependent on student choice     |

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

### Foundation Year Availability Code Values

| foundation_year_availability.code value | foundation_year_availability.label value |
|-----------------------------------------|------------------------------------------|
| 0                                       | Not available                            |
| 1                                       | Optional                                 |
| 2                                       | Compulsory                               |

### Length of Course Code Values

| length_of_course.code value | length_of_course.label value |
|-----------------------------|------------------------------|
| 1                           | 1 stage                      |
| 2                           | 2 stages                     |
| 3                           | 3 stages                     |
| 4                           | 4 stages                     |
| 5                           | 5 stages                     |
| 6                           | 6 stages                     |
| 7                           | 7 stages                     |

### Mode Code Values

| mode.code value | mode.label value |
|-----------------|------------------|
| 1               | Full-time        |
| 2               | Part-Time        |
| 3               | Both             |

### NHS Code Values

| nhs.code value | nhs.label value |
|----------------|-----------------|
| 0              | None            |
| 1              | Any             |

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
