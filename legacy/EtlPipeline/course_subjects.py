from typing import Any
from typing import Dict
from typing import List
from typing import Union


def get_subjects(raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts and transforms the SBJ entries in a KISCOURSE.
    Returns subject data as a list of dictionaries.

    :param raw_course_data: Course dictionary containing subject data
    :type raw_course_data: Dict[str, Any]
    :return: List of subject data as dictionaries
    :rtype: List[Dict[str, Any]]
    """

    subjects = []
    subject_codes = raw_course_data["SBJ"]
    code_list = convert_to_list(subject_codes)

    for code in code_list:
        subject = {"code": code}

        subjects.append(subject)

    return subjects


def convert_to_list(subject_codes: Union[str, Any]) -> Union[List[str], Any]:
    """
    If value is a str, return in list, otherwise return value

    :param subject_codes: Value to check
    :type subject_codes: Union[str, Any]
    :return: Value as a list if it's a string, otherwise the value
    :rtype: Union[List[str], Any]
    """
    if isinstance(subject_codes, str):
        return [subject_codes]

    return subject_codes
