import datetime
import uuid


def build_subject_doc(subject_list):

    doc = {
        "id": str(uuid.uuid1()),
        "code": subject_list[0],
        "english_name": subject_list[1],
        "level": int(subject_list[2]),
        "updated_at": datetime.datetime.utcnow().isoformat(),
        "welsh_name": subject_list[3],
    }

    return doc
