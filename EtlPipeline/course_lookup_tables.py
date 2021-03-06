"""This module provides lookup tables for use when building course docs"""

accreditation_code = {
    0: "Accreditation is not dependent on student choice",
    1: "Accreditation is dependent on student choice",
}

country_code = {
    "XF": "England",
    "XG": "Northern Ireland",
    "XH": "Scotland",
    "XI": "Wales",
}

length_of_course = {
    1: "1 stage",
    2: "2 stages",
    3: "3 stages",
    4: "4 stages",
    5: "5 stages",
    6: "6 stages",
    7: "7 stages",
}

distance_learning_lookup = {
    0: "Course is available other than by distance learning",
    1: "Course is only available through distance learning",
    2: "Course is optionally available through distance learning",
}

foundation_year_availability = {
    0: "Not available",
    1: "Optional",
    2: "Compulsory",
}

mode = {1: "Full-time", 2: "Part-time"}

nhs_funded = {0: "None", 1: "Any"}

sandwich_year = {0: "Not available", 1: "Optional", 2: "Compulsory"}

year_abroad = {0: "Not available", 1: "Optional", 2: "Compulsory"}
