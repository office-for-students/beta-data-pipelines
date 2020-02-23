### Course Data

#### Cosmos db doc:

```json5
{
    "_id": "string",
    "builds": {
        "courses": {
            "number_of_resources_loaded": "integer",
            "status": "string"
        },
        "institutions": {
            "number_of_resources_loaded": "integer",
            "status": "string"
        },
        "search": {
            "number_of_resources_loaded": "integer",
            "status": "string"
        },
        "subjects": {
            "number_of_resources_loaded": "integer",
            "status": "string"
        }
    },
    "created_at": "date",
    "status": "string",
    "total_number_of_courses": "integer",
    "total_number_of_institutions": "integer",
    "version": "integer"
}
```

#### JSON field defaults

| JSON field name                                | Default Value              |
|------------------------------------------------|----------------------------|
| builds.courses.number_of_resources_loaded      | 0                          |
| builds.courses.status                          | pending                    |
| builds.institutions.number_of_resources_loaded | 0                          |
| builds.institutions.status                     | pending                    |
| builds.search.number_of_resources_loaded       | 0                          |
| builds.search.status                           | pending                    |
| builds.subjects.number_of_resources_loaded     | 0                          |
| builds.subjects.status                         | pending                    |
| created_at                                     | datetimestamp              |
| status                                         | in-progress                |
| total_number_of_courses                        | To be calculated           |
| total_number_of_institutions                   | To be calculated           |
| version                                        | Append 1 to latest version |

