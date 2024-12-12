import logging
import time

from azure.cosmos import ContainerProxy

from constants import COSMOS_DATABASE_ID
from . import models


def load_subject_documents(
        rows: list[list[str]],
        version: int,
        collection_link: str,
        collection_container: ContainerProxy
) -> None:
    """
    Loads subject data from the CSV rows and stores it in the database
    """
    _collection_link = collection_link
    _container = collection_container
    _db_id = COSMOS_DATABASE_ID
    _rows = rows
    _version = version

    sproc_link = _collection_link + "/sprocs/bulkImport"
    partition_key = str(_version)

    subject_count = 0
    new_docs = []
    sproc_count = 0

    for row in _rows:
        subject_count += 1
        sproc_count += 1

        if subject_count == 1:
            logging.info("skipping header row")
            continue

        # Transform row into json object
        new_docs.append(models.build_subject_doc(row, _version))

        if sproc_count == 100:
            logging.info(f"Beginning execution of stored procedure for {sproc_count} documents")

            _container.scripts.execute_stored_procedure(
                sproc=sproc_link,
                params=[new_docs],
                partition_key=partition_key
            )
            logging.info(f"Successfully loaded another {sproc_count} documents")
            # Reset values
            new_docs = []
            sproc_count = 0
            time.sleep(10)

    if sproc_count > 0:
        logging.info(f"Beginning execution of stored procedure for {sproc_count} documents")
        _container.scripts.execute_stored_procedure(
            sproc=sproc_link,
            params=[new_docs],
            partition_key=partition_key
        )
        logging.info(f"Successfully loaded another {sproc_count} documents")

    logging.info(f"loaded {subject_count - 1} into {_collection_link} collection in {_db_id} database")
