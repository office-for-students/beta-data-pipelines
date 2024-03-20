import unittest
from unittest import mock

from legacy.services.dataset_service import DataSetService


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
    @mock.patch("SharedCode.dataset_helper.get_cosmos_client")
    def test_initialisation(self, mock_get_cosmos_client):
        mock_get_cosmos_client.return_value = mock.MagicMock()
        try:
            DataSetService()
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
    @mock.patch("SharedCode.dataset_helper.get_cosmos_client")
    def test_update_status(self, mock_get_cosmos_client):
        dataset_service = DataSetService()

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {"courses": {"status": "pending"}}
        latest_dataset_doc["updated_at"] = "dave"
        dataset_service.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)

        dataset_service.cosmos_client.UpsertItem = mock.MagicMock()

        dataset_service.update_status("courses", "in progress", "dave")

        expected_connection_link = (
            "dbs/test-db-id/colls/test-dataset-collection-id"
        )
        expected_dataset_doc = {}
        expected_dataset_doc["version"] = 3
        expected_dataset_doc["builds"] = {"courses": {"status": "in progress"}}
        expected_dataset_doc["updated_at"] = "dave"
        dataset_service.cosmos_client.UpsertItem.assert_called_once_with(
            expected_connection_link, expected_dataset_doc
        )

    @mock.patch.dict(
        "os.environ",
        values={
            "AzureCosmosDbDatabaseId": "test-db-id",
            "AzureCosmosDbDataSetCollectionId": "test-dataset-collection-id",
        },
        clear=True,
    )
    @mock.patch("SharedCode.dataset_helper.get_cosmos_client")
    def test_have_all_builds_succeeded_with_all_pending(
            self, mock_get_cosmos_client
    ):
        dataset_service = DataSetService()

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
    @mock.patch("SharedCode.dataset_helper.get_cosmos_client")
    def test_have_all_builds_succeeded_with_one_pending(
            self, mock_get_cosmos_client
    ):
        dataset_service = DataSetService()

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
    @mock.patch("SharedCode.dataset_helper.get_cosmos_client")
    def test_have_all_builds_succeeded_with_two_pending(
            self, mock_get_cosmos_client
    ):
        dataset_service = DataSetService()

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
    @mock.patch("SharedCode.dataset_helper.get_cosmos_client")
    def test_have_all_builds_succeeded_with_all_succeeded(
            self, mock_get_cosmos_client
    ):
        dataset_service = DataSetService()

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
