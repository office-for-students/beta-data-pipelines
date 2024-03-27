import logging
import time
from typing import Iterable

from azure.cosmos import ContainerProxy

from constants import COSMOS_COLLECTION_SUBJECTS
from constants import COSMOS_DATABASE_ID
from . import models
from services.cosmosservice import CosmosService


def load_collection(rows: Iterable, version: int, cosmos_service: CosmosService) -> None:
    """
    Creates a loader class with the given parameters, used to load subjects into the dataset

    :param rows: Rows from CSV containing subject data
    :type rows: Iterable[str]
    :param version: Version of the dataset
    :type version: int
    :param cosmos_service: Cosmos service to load collection from
    :type cosmos_service: CosmosService
    :return: None
    """
    loader = Loader(
        rows=rows,
        version=version,
        cosmos_service=cosmos_service
    )
    loader.load_subject_documents()


class Loader:
    def __init__(
            self,
            rows: Iterable,
            version: int,
            cosmos_service: CosmosService,
            collection_id: str = COSMOS_COLLECTION_SUBJECTS
    ) -> None:

        self.cosmos_service = cosmos_service
        self.collection_id = collection_id
        self.collection_link = self.cosmos_service.get_collection_link(collection_id)
        self.container: ContainerProxy = cosmos_service.get_container(container_id=self.collection_id)
        self.db_id = COSMOS_DATABASE_ID
        self.rows = rows
        self.version = version

    def load_subject_documents(self) -> None:
        """
        Loads subject data from the CSV rows and stores it in the database
        """
        sproc_link = self.collection_link + "/sprocs/bulkImport"
        partition_key = str(self.version)

        subject_count = 0
        new_docs = []
        sproc_count = 0

        for row in self.rows:
            subject_count += 1
            sproc_count += 1

            if subject_count == 1:
                logging.info("skipping header row")
                continue

            # Transform row into json object
            new_docs.append(models.build_subject_doc(row, self.version))

            if sproc_count == 100:
                logging.info(f"Beginning execution of stored procedure for {sproc_count} documents")

                self.container.scripts.execute_stored_procedure(
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
            self.container.scripts.execute_stored_procedure(
                sproc=sproc_link,
                params=[new_docs],
                partition_key=partition_key
            )
            logging.info(f"Successfully loaded another {sproc_count} documents")

        logging.info(f"loaded {subject_count - 1} into {self.collection_id} collection in {self.db_id} database")
