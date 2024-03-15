import datetime
import uuid
from typing import Any
from typing import Dict
from typing import List


def build_subject_doc(subject_list: List[str], version: int) -> Dict[str, Any]:

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
