import unittest
from unittest import mock

from services.cosmos_service import get_current_provider
from services.dataset_service.general import DataSetService
from azure.cosmos import ContainerProxy

# import SharedCode


class TestDataSetHelper(unittest.TestCase):
    @mock.patch.dict(
        "os.environ",
        values={
            "AzureCosmosDbDatabaseId": "test-db-id",
            "AzureCosmosDbDataSetCollectionId": "test-dataset-collection-id",
        },
        clear=True,
    )
    @mock.patch("services.cosmos_service.get_current_provider")
    def test_initialisation(self, mock_get_cosmos_client):
        mock_get_cosmos_client.return_value = mock.MagicMock()
        try:
            DataSetService(cosmos_container=mock.MagicMock(ContainerProxy))
        except Exception as e:
            self.fail(
                f"DataSetHelper initialisation raised unexpected Exception: {e}"
            )

    @mock.patch.dict(
        "os.environ",
        values={
            "AzureCosmosDbDatabaseId": "test-db-id",
            "AzureCosmosDbDataSetCollectionId": "test-dataset-collection-id",
        },
        clear=True,
    )

    def test_update_status(self):
        dataset_service = DataSetService(cosmos_container=mock.MagicMock(ContainerProxy))
        dataset_service.container.upsert_item = mock.MagicMock()

        latest_dataset_doc = dict()
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {"courses": {"status": "pending"}}
        latest_dataset_doc["updated_at"] = "dave"
        dataset_service.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)

        dataset_service.update_status("courses", "in progress", "dave")

        expected_dataset_doc = dict()
        expected_dataset_doc["version"] = 3
        expected_dataset_doc["builds"] = {"courses": {"status": "in progress"}}
        expected_dataset_doc["updated_at"] = "dave"
        dataset_service.container.upsert_item.assert_called_once_with(
             expected_dataset_doc
        )

    @mock.patch.dict(
        "os.environ",
        values={
            "AzureCosmosDbDatabaseId": "test-db-id",
            "AzureCosmosDbDataSetCollectionId": "test-dataset-collection-id",
        },
        clear=True,
    )

    def test_have_all_builds_succeeded_with_all_pending(
            self
    ):
        dataset_service = DataSetService(cosmos_container=mock.MagicMock(ContainerProxy))

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "pending"},
            "institutions": {"status": "pending"},
            "search": {"status": "pending"},
            "subjects": {"status": "pending"},
        }
        dataset_service.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertFalse(dataset_service.have_all_builds_succeeded())

    @mock.patch.dict(
        "os.environ",
        values={
            "AzureCosmosDbDatabaseId": "test-db-id",
            "AzureCosmosDbDataSetCollectionId": "test-dataset-collection-id",
        },
        clear=True,
    )

    def test_have_all_builds_succeeded_with_one_pending(
            self
    ):
        dataset_service = DataSetService(cosmos_container=mock.MagicMock(ContainerProxy))

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "pending"},
            "institutions": {"status": "succeeded"},
            "search": {"status": "succeeded"},
            "subjects": {"status": "succeeded"},
        }
        dataset_service.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertFalse(dataset_service.have_all_builds_succeeded())

    @mock.patch.dict(
        "os.environ",
        values={
            "AzureCosmosDbDatabaseId": "test-db-id",
            "AzureCosmosDbDataSetCollectionId": "test-dataset-collection-id",
        },
        clear=True,
    )

    def test_have_all_builds_succeeded_with_two_pending(
            self
    ):
        dataset_service = DataSetService(cosmos_container=mock.MagicMock(ContainerProxy))

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "pending"},
            "institutions": {"status": "pending"},
            "search": {"status": "succeeded"},
            "subjects": {"status": "succeeded"},
        }
        dataset_service.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertFalse(dataset_service.have_all_builds_succeeded())

    @mock.patch.dict(
        "os.environ",
        values={
            "AzureCosmosDbDatabaseId": "test-db-id",
            "AzureCosmosDbDataSetCollectionId": "test-dataset-collection-id",
        },
        clear=True,
    )

    def test_have_all_builds_succeeded_with_all_succeeded(
            self
    ):
        dataset_service = DataSetService(cosmos_container=mock.MagicMock(ContainerProxy))

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "succeeded"},
            "institutions": {"status": "succeeded"},
            "search": {"status": "succeeded"},
            "subjects": {"status": "succeeded"},
        }
        dataset_service.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertTrue(dataset_service.have_all_builds_succeeded())


if __name__ == "__main__":
    unittest.main()
