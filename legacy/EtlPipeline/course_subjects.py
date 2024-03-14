from typing import Any
from typing import Dict
from typing import List
from typing import Union


def get_subjects(raw_course_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts and transforms the SBJ entries in a KISCOURSE"""

    subjects = []
    subject_codes = raw_course_data["SBJ"]
    code_list = convert_to_list(subject_codes)

    for code in code_list:
        subject = {"code": code}

        subjects.append(subject)

    return subjects


def convert_to_list(subject_codes: Union[str, Any]) -> Union[List[str], Any]:
    """If value is a dict, return in list, otherwise return value"""
    if isinstance(subject_codes, str):
        return [subject_codes]

    return subject_codes
