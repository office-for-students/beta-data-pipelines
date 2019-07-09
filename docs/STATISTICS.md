### Statistics Data

```json5
...
    "statistics": {
        "continuation": [
            {
                "aggregation_level": "integer",
                "continuing_with_provider": "integer",
                "dormant": "integer",
                "gained": "integer",
                "lower": "integer",
                "left": "integer",
                "number_of_students": "integer",
                "subject": {
                    "code": "string",
                    "english_label": "string",
                    "welsh_label": "string"
                },
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ],
        "employment": [
            {
                "aggregation_level": "integer",
                "number_of_students": "integer",
                "assumed_to_be_unemployed": "integer",
                "in_study": "integer",
                "in_work": "integer",
                "in_work_and_study": "integer",
                "in_work_or_study": "integer",
                "not_available_for_work_or_study": "integer",
                "response_rate": "integer",
                "subject": {
                    "code": "string",
                    "english_label": "string",
                    "welsh_label": "string"
                },
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ],
        "entry": [
            {
                "aggregation_level": "integer",
                "number_of_students": "integer",
                "a-level": "integer",
                "access": "integer",
                "another_higher_education_qualifications": "integer",
                "baccalaureate": "integer",
                "degree": "integer",
                "foundation": "integer",
                "none": "integer",
                "other_qualifications": "integer",
                "subject": {
                    "code": "string",
                    "welsh_label": "string",
                    "english_label": "string"
                },
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ],
        "job_type": [
            {
                "aggregation_level": "integer",
                "number_of_students": "integer",
                "proportion_of_students_in_professional_or_managerial_jobs": "integer",
                "proportion_of_students_in_non_professional_or_managerial_jobs": "integer",
                "proportion_of_students_in_unknown_professions": "integer",
                "response_rate": "integer",
                "subject": {
                    "code": "string",
                    "english_label": "string",
                    "welsh_label": "string"
                },
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ],
        "job_list": {
            "items": [
                {
                    "aggregation_level": "integer",
                    "list": [
                        {
                            "job": "string",
                            "percentage_of_students": "integer"
                        }
                    ],
                    "number_of_students": "integer",
                    "order": "integer",
                    "response_rate": "integer",
                    "subject": {
                        "code": "string",
                        "english_label": "string",
                        "welsh_label": "string"
                    },
                    "unavailable": {
                        "code": "integer",
                        "reason": "string"
                    }
                }
            ],
            "unavailable": {
                "code": "integer",
                "reason": "string"
            }
        },
        "leo": [
            {
                "aggregation_level": "integer",
                "higher_quartile_range": "integer",
                "lower_quartile_range": "integer",
                "median": "integer",
                "number_of_graduates": "integer",
                "subject": {
                    "code": "string",
                    "english_label": "string",
                    "welsh_label": "string"
                },
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ],
        "nss": [
            {
                "aggregation_level": "integer",
                "number_of_students": "integer",
                "question_1": {
                    "description": "string",
                    "agree_or_strongly_agree": "integer"
                },
                "question_2": {
                    "description": "string",
                    "agree_or_strongly_agree": "integer"
                },
                ...
                "question_27": {
                    "description": "string",
                    "agree_or_strongly_agree": "integer"
                },
                "response_rate": "integer",
                "subject": {
                    "code": "string",
                    "english_label": "string",
                    "welsh_label": "string"
                },
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ],
        "salary": [
            {
                "aggregation_level": "integer",
                "number_of_students": "integer",
                "response_rate": "integer",
                "subject": {
                    "code": "string",
                    "english_label": "string",
                    "welsh_label": "string"
                },
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ],
        "tariff": [
            {
                "aggregation_level": "integer",
                "number_of_students": "integer",
                "subject": {
                    "code": "string",
                    "english_label": "string",
                    "welsh_label": "string"
                },
                "tariffs": [
                    {
                        "code": "string",
                        "description": "string",
                        "entrants": "integer"
                    }
                ],
                "unavailable": {
                    "code": "integer",
                    "reason": "string"
                }
            }
        ]
    },
...
```

#### Mapping Xml fields to JSON fields (Starting path is KIS.INSTITUTION)

| JSON field name                                               | XML Path                                     |  XML field name   |
|---------------------------------------------------------------|----------------------------------------------|-------------------|
| statistics.continuation.[].aggregation_level                  | ...KISCOURSE.CONTINUATION.CONTAGG            | CONTAGG           |
| statistics.continuation.[].number_of_students                 | ...KISCOURSE.CONTINUATION.CONTPOP            | CONTPOP           |
| statistics.continuation.[].continuing_with_provider           | ...KISCOURSE.CONTINUATION.UCONT              | UCONT             |
| statistics.continuation.[].dormant                            | ...KISCOURSE.CONTINUATION.UDORMANT           | UDORMANT          |
| statistics.continuation.[].gained                             | ...KISCOURSE.CONTINUATION.UGAINED            | UGAINED           |
| statistics.continuation.[].lower                              | ...KISCOURSE.CONTINUATION.ULOWER             | ULOWER            |
| statistics.continuation.[].left                               | ...KISCOURSE.CONTINUATION.ULEFT              | ULEFT             |
| statistics.continuation.[].subject.code                       | ...KISCOURSE.CONTINUATION.CONTSBJ            | CONTSBJ           |
| statistics.continuation.[].subject.english_label              | See list of subjects                         | N/A               |
| statistics.continuation.[].subject.welsh_label                | See list of subjects                         | N/A               |
| statistics.continuation.[].unavailable.code                   | ...KISCOURSE.CONTINUATION.CONTUNAVAILREASON  | CONTUNAVAILREASON |
| statistics.continuation.[].unavailable.reason                 | See aggregation analysis file (DLHE)         | N/A               |
| statistics.employment.[].aggregation_level                    | ...KISCOURSE.EMPLOYMENT.EMPAGG               | EMPAGG            |
| statistics.employment.[].number_of_students                   | ...KISCOURSE.EMPLOYMENT.EMPPOP               | EMPPOP            |
| statistics.employment.[].assumed_to_be_unemployed             | ...KISCOURSE.EMPLOYMENT.ASSUNEMP             | ASSUNEMP          |
| statistics.employment.[].in_study                             | ...KISCOURSE.EMPLOYMENT.STUDY                | STUDY             |
| statistics.employment.[].in_work                              | ...KISCOURSE.EMPLOYMENT.WORK                 | WORK              |
| statistics.employment.[].in_work_and_study                    | ...KISCOURSE.EMPLOYMENT.BOTH                 | BOTH              |
| statistics.employment.[].in_work_or_study                     | ...KISCOURSE.EMPLOYMENT.WORKSTUDY            | WORKSTUDY         |
| statistics.employment.[].not_available_for_work_or_study      | ...KISCOURSE.EMPLOYMENT.NOAVAIL              | NOAVAIL           |
| statistics.employment.[].response_rate                        | ...KISCOURSE.EMPLOYMENT.EMPRESP_RATE         | EMPRESP_RATE      |
| statistics.employment.[].subject.code                         | ...KISCOURSE.EMPLOYMENT.EMPSBJ               | EMPSBJ            |
| statistics.employment.[].subject.english_label                | See list of subjects                         | N/A               |
| statistics.employment.[].subject.welsh_label                  | See list of subjects                         | N/A               |
| statistics.employment.[].unavailable.code                     | ...KISCOURSE.EMPLOYMENT.EMPUNAVAILREASON     | EMPUNAVAILREASON  |
| statistics.employment.[].unavailable.reason                   | See aggregation analysis file (DLHE)         | N/A               |
| statistics.entry.[].aggregation_level                         | ...KISCOURSE.ENTRY.ENTAGG                    | ENTAGG            |
| statistics.entry.[].number_of_students                        | ...KISCOURSE.ENTRY.ENTPOP                    | ENTPOP            |
| statistics.entry.[].a-level                                   | ...KISCOURSE.ENTRY.ALEVEL                    | ALEVEL            |
| statistics.entry.[].access                                    | ...KISCOURSE.ENTRY.ACCESS                    | ACCESS            |
| statistics.entry.[].another_higher_education_qualifications   | ...KISCOURSE.ENTRY.OTHERHE                   | OTHERHE           |
| statistics.entry.[].baccalaureate                             | ...KISCOURSE.ENTRY.BACC                      | BACC              |
| statistics.entry.[].degree                                    | ...KISCOURSE.ENTRY.DEGREE                    | DEGREE            |
| statistics.entry.[].foundation                                | ...KISCOURSE.ENTRY.FOUNDTN                   | FOUNDTN           |
| statistics.entry.[].none                                      | ...KISCOURSE.ENTRY.NOQUALS                   | NOQUALS           |
| statistics.entry.[].other_qualifications                      | ...KISCOURSE.ENTRY.OTHER                     | OTHER             |
| statistics.entry.[].subject.code                              | ...KISCOURSE.ENTRY.ENTSBJ                    | ENTSBJ            |
| statistics.entry.[].subject.english_label                     | See list of subjects                         | N/A               |
| statistics.entry.[].subject.welsh_label                       | See list of subjects                         | N/A               |
| statistics.entry.[].unavailable.code                          | ...KISCOURSE.ENTRY.ENTUNAVAILREASON          | ENTUNAVAILREASON  |
| statistics.entry.[].unavailable.reason                        | See aggregation analysis file (DLHE)         | N/A               |
| statistics.job_type.[].aggregation_level                      | ...KISCOURSE.JOBTYPE.JOBAGG                  | JOBAGG            |
| statistics.job_type.[].number_of_students                     | ...KISCOURSE.JOBTYPE.JOBPOP                  | JOBPOP            |
| statistics.job_type.[].in_professional_or_managerial_jobs     | ...KISCOURSE.JOBTYPE.PROFMAN                 | PROFMAN           |
| statistics.job_type.[].in_non_professional_or_managerial_jobs | ...KISCOURSE.JOBTYPE.OTHERJOB                | OTHERJOB          |
| statistics.job_type.[].in_unknown_professions                 | ...KISCOURSE.JOBTYPE.UNKWN                   | UNKWN             |
| statistics.job_type.[].response_rate                          | ...KISCOURSE.JOBTYPE.JOBRESP_RATE            | JOBRESP_RATE      |
| statistics.job_type.[].subject.code                           | ...KISCOURSE.JOBTYPE.JOBSBJ                  | JOBSBJ            |
| statistics.job_type.[].subject.english_label                  | See list of subjects                         | N/A               |
| statistics.job_type.[].subject.welsh_label                    | See list of subjects                         | N/A               |
| statistics.job_type.[].unavailable.code                       | ...KISCOURSE.JOBTYPE.JOBUNAVAILREASON        | JOBUNAVAILREASON  |
| statistics.job_type.[].unavailable.reason                     | See aggregation analysis file (DLHE)         | N/A               |
| statistics.job_list.items.[].aggregation_level                | ...KISCOURSE.COMMON.COMAGG                   | COMAGG            |
| statistics.job_list.items.[].number_of_students               | ...KISCOURSE.COMMON.COMPOP                   | COMPOP            |
| statistics.job_list.items.[].list.[].order                    | ...KISCOURSE.COMMON.JOBLIST.ORDER            | ORDER             |
| statistics.job_list.items.[].list.[].job                      | ...KISCOURSE.COMMON.JOBLIST.JOB              | JOB               |
| statistics.job_list.items.[].list.[].percentage_of_students   | ...KISCOURSE.COMMON.JOBLIST.PERC             | PERC              |
| statistics.job_list.items.[].response_rate                    | ...KISCOURSE.COMMON.COMRESP_RATE             | COMRESP_RATE      |
| statistics.job_list.items.[].subject.code                     | ...KISCOURSE.COMMON.COMSBJ                   | COMSBJ            |
| statistics.job_list.items.[].subject.english_label            | See list of subjects                         | N/A               |
| statistics.job_list.items.[].subject.welsh_label              | See list of subjects                         | N/A               |
| statistics.job_list.items.[].unavailable.code                 | ...KISCOURSE.COMMON.COMUNAVAILREASON         | COMUNAVAILREASON  |
| statistics.job_list.items.[].unavailable.reason               | See aggregation analysis file (DLHE)         | N/A               |
| statistics.job_list.unavailable.code                          | ...KISCOURSE.COMMON.COMUNAVAILREASON         | COMUNAVAILREASON  |
| statistics.job_list.unavailable.reason                        | See aggregation analysis file (DLHE)         | N/A               |
| statistics.leo.[].aggregation_level                           | ...KISCOURSE.LEO.LEOAGG                      | LEOAGG            |
| statistics.leo.[].number_of_graduates                         | ...KISCOURSE.LEO.LEOPOP                      | LEOPOP            |
| statistics.leo.[].higher_quartile                             | ...KISCOURSE.LEO.LEOUQ                       | LEOUQ             |
| statistics.leo.[].lower_quartile                              | ...KISCOURSE.LEO.LEOLQ                       | LEOLQ             |
| statistics.leo.[].median                                      | ...KISCOURSE.LEO.LEOMED                      | LEOMED            |
| statistics.leo.[].subject.code                                | ...KISCOURSE.LEO.LEOSBJ                      | LEOSBJ            |
| statistics.leo.[].subject.english_label                       | See list of subjects                         | N/A               |
| statistics.leo.[].subject.welsh_label                         | See list of subjects                         | N/A               |
| statistics.leo.[].unavailable.code                            | ...KISCOURSE.LEO.LEOUNAVAILREASON            | LEOUNAVAILREASON  |
| statistics.leo.[].unavailable.reason                          | See aggregation analysis file (DLHE) (LEO)          | N/A               |
| statistics.nss.[].aggregation_level                           | ...KISCOURSE.NSS.NSSAGG                      | NSSAGG            |
| statistics.nss.[].number_of_students                          | ...KISCOURSE.NSS.NSSPOP                      | NSSPOP            |
| statistics.nss.[].response_rate                               | ...KISCOURSE.NSS.NSSRESP_RATE                | NSSRESP_RATE      |
| statistics.nss.[].question_<n>.agree_or_strongly_agree        | ...KISCOURSE.NSS.Q<n>                        | Q1,Q2,...,Q27     |
| statistics.nss.[].question_<n>.question                       | See Question Number mappings                 | N/A               |
| statistics.nss.[].subject.code                                | ...KISCOURSE.NSS.NSSSBJ                      | NSSSBJ            |
| statistics.nss.[].subject.english_label                       | See list of subjects                         | N/A               |
| statistics.nss.[].subject.welsh_label                         | See list of subjects                         | N/A               |
| statistics.nss.[].unavailable.code                            | ...KISCOURSE.NSS.NSSUNAVAILREASON            | NSSUNAVAILREASON  |
| statistics.nss.[].unavailable.reason                          | See aggregation analysis file (DLHE)         | N/A               |
| statistics.salary.[].aggregation_level                        | ...KISCOURSE.SALARY.SALAGG                   | SALAGG            |
| statistics.salary.[].number_of_graduates                      | ...KISCOURSE.SALARY.SALPOP                   | SALPOP            |
| statistics.salary.[].higher_quartile                          | ...KISCOURSE.SALARY.UQ                       | UQ                |
| statistics.salary.[].lower_quartile                           | ...KISCOURSE.SALARY.LQ                       | LQ                |
| statistics.salary.[].median                                   | ...KISCOURSE.SALARY.MED                      | LEOMED            |
| statistics.salary.[].response_rate                            | ...KISCOURSE.SALARY.SALRESP_RATE             | SALRESP_RATE      |
| statistics.salary.[].subject.code                             | ...KISCOURSE.SALARY.SALSBJ                   | SALSBJ            |
| statistics.salary.[].subject.english_label                    | See list of subjects                         | N/A               |
| statistics.salary.[].subject.welsh_label                      | See list of subjects                         | N/A               |
| statistics.salary.[].unavailable.code                         | ...KISCOURSE.SALARY.SALUNAVAILREASON         | SALUNAVAILREASON  |
| statistics.salary.[].unavailable.reason                       | See aggregation analysis file (DLHE)         | N/A               |
| statistics.tariff.[].aggregation_level                        | ...KISCOURSE.TARIFF.TARAGG                   | TARAGG            |
| statistics.tariff.[].number_of_students                       | ...KISCOURSE.TARIFF.TARPOP                   | TARPOP            |
| statistics.tariff.[].tariffs.[].code                          | ...KISCOURSE.TARIFF.T001 (Use key not value) | T001              |
| statistics.tariff.[].tariffs.[].label                         | See Tariff Code values                       | N/A               |
| statistics.tariff.[].tariffs.[].entrants                      | ...KISCOURSE.TARIFF.T001                     | T001,T048,...     |
| statistics.tariff.[].subject.code                             | ...KISCOURSE.TARIFF.TARSBJ                   | TARSBJ            |
| statistics.tariff.[].subject.english_label                    | See list of subjects                         | N/A               |
| statistics.tariff.[].subject.welsh_label                      | See list of subjects                         | N/A               |
| statistics.tariff.[].unavailable.code                         | ...KISCOURSE.TARIFF.TARUNAVAILREASON         | TARUNAVAILREASON  |
| statistics.tariff.[].unavailable.reason                       | See aggregation analysis file (DLHE)         | N/A               |

### Question Number mappings

| question_n| question_n.question value                                                                                               |
|-------------|---------------------------------------------------------------------------------------------------------------------------|
| 1           | Staff are good at explaining things                                                                                       |
| 2           | Staff have made the subject interesting                                                                                   |
| 3           | The course is intellectually stimulating                                                                                  |
| 4           | My course has challenged me to achieve my best work                                                                       |
| 5           | My course has provided me with opportunities to explore ideas or concepts in depth                                        |
| 6           | My course has provided me with opportunities to bring information and ideas together from different topics                |
| 7           | My course has provided me with opportunities to apply what I have learnt                                                  |
| 8           | The criteria used in marking have been clear in advance                                                                   |
| 9           | Marking and assessment has been fair                                                                                      |
| 10          | Feedback on my work has been timely                                                                                       |
| 11          | I have received helpful comments on my work                                                                               |
| 12          | I have been able to contact staff when I needed to                                                                        |
| 13          | I have received sufficient advice and guidance in relation to my course                                                   |
| 14          | Good advice was available when I needed to make study choices on my course                                                |
| 15          | The course is well organised and running smoothly                                                                         |
| 16          | The timetable works efficiently for me                                                                                    |
| 17          | Any changes in the course or teaching have been communicated effectively                                                  |
| 18          | The IT resources and facilities provided have supported my learning well                                                  |
| 19          | The library resources (e.g. books, online services and learning spaces) have supported my learning well                   |
| 20          | I have been able to access course-specific resources (e.g. equipment, facilities, software, collections) when I needed to |
| 21          | I feel part of a community of staff and students                                                                          |
| 22          | I have had the right opportunities to work with other students as part of my course                                       |
| 23          | I have had the right opportunities to provide feedback on my course                                                       |
| 24          | Staff value students' views and opinions about the course                                                                 |
| 25          | It is clear how students' feedback on the course has been acted on                                                        |
| 26          | The students' union (association or guild) effectively represents students' academic interests                            |
| 27          | Overall, I am satisfied with the quality of the course                                                                    |

### Tariff Code values

| tariffs.[].code value | tariffs.[].label value            |
|-----------------------|-----------------------------------|
| T001                  | less than 48 tariff points        |
| T048                  | between 48 and 63 tariff points   |
| T064                  | between 64 and 79 tariff points   |
| T080                  | between 80 and 95 tariff points   |
| T096                  | between 96 and 111 tariff points  |
| T112                  | between 112 and 127 tariff points |
| T128                  | between 128 and 143 tariff points |
| T144                  | between 144 and 159 tariff points |
| T160                  | between 160 and 175 tariff points |
| T176                  | between 176 and 191 tariff points |
| T192                  | between 192 and 207 tariff points |
| T208                  | between 208 and 239 tariff points |
| T240                  | more than 240 tariff points       |