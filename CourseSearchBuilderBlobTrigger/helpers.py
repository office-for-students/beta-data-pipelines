import logging
from SharedCode import exceptions


def get_version(dataset_path):
    version = None
    try:
        split_path_name = dataset_path.split("/")
        split_name = split_path_name[1].split("-")
        version = split_name[2]
    except IndexError:
        logging.error(
            f"unable to extract version from storage container path, \
                    dataset_path: {dataset_path}\n"
        )
        raise exceptions.StopEtlPipelineErrorException(IndexError)

    return version
