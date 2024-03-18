import datetime
import uuid
from typing import Any
from typing import Dict
from typing import List


def build_subject_doc(subject_list: List[str], version: int) -> Dict[str, Any]:
    """
    Takes a list of subject data and the version number of the data set and returns a dictionary containing keyed data.

    :param subject_list: List containing subject data
    :type subject_list: List[str]
    :param version: Version number of the data set
    :type version: int
    :return: Dictionary containing subject data
    :rtype: Dict[str, Any]
    """
    doc = {
        "id": str(uuid.uuid1()),
        "code": subject_list[0],
        "english_name": subject_list[1],
        "level": int(subject_list[2]),
        "updated_at": datetime.datetime.utcnow().isoformat(),
        "welsh_name": subject_list[3],
        "partition_key": str(version),
        "version": version,
    }

    return doc
