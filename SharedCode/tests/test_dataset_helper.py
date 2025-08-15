import unittest
from unittest import mock

# import SharedCode
from SharedCode.dataset_helper import DataSetHelper


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
        # Mock the CosmosClient and its methods
        mock_cosmos_client = mock.MagicMock()
        mock_database_client = mock.MagicMock()
        mock_container_client = mock.MagicMock()

        # Set up the mock chain
        mock_get_cosmos_client.return_value = mock_cosmos_client
        mock_cosmos_client.get_database_client.return_value = mock_database_client
        mock_database_client.get_container_client.return_value = mock_container_client

        try:
            DataSetHelper()
        except:
            self.fail(
                "DataSetHelper initialisation raised unexpected Exception"
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
        # Mock the CosmosClient and its methods
        mock_cosmos_client = mock.MagicMock()
        mock_database_client = mock.MagicMock()
        mock_container_client = mock.MagicMock()

        # Set up the mock chain
        mock_get_cosmos_client.return_value = mock_cosmos_client
        mock_cosmos_client.get_database_client.return_value = mock_database_client
        mock_database_client.get_container_client.return_value = mock_container_client

        dsh = DataSetHelper()

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {"courses": {"status": "pending"}}
        latest_dataset_doc["updated_at"] = "dave"
        dsh.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)

        dsh.container.upsert_item = mock.MagicMock()

        dsh.update_status("courses", "in progress", "dave")

        expected_dataset_doc = {}
        expected_dataset_doc["version"] = 3
        expected_dataset_doc["builds"] = {"courses": {"status": "in progress"}}
        expected_dataset_doc["updated_at"] = "dave"
        dsh.container.upsert_item.assert_called_once_with(expected_dataset_doc)

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
        # Mock the CosmosClient and its methods
        mock_cosmos_client = mock.MagicMock()
        mock_database_client = mock.MagicMock()
        mock_container_client = mock.MagicMock()

        # Set up the mock chain
        mock_get_cosmos_client.return_value = mock_cosmos_client
        mock_cosmos_client.get_database_client.return_value = mock_database_client
        mock_database_client.get_container_client.return_value = mock_container_client

        dsh = DataSetHelper()

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "pending"},
            "institutions": {"status": "pending"},
            "search": {"status": "pending"},
            "subjects": {"status": "pending"},
        }
        dsh.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertFalse(dsh.have_all_builds_succeeded())

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
        # Mock the CosmosClient and its methods
        mock_cosmos_client = mock.MagicMock()
        mock_database_client = mock.MagicMock()
        mock_container_client = mock.MagicMock()

        # Set up the mock chain
        mock_get_cosmos_client.return_value = mock_cosmos_client
        mock_cosmos_client.get_database_client.return_value = mock_database_client
        mock_database_client.get_container_client.return_value = mock_container_client

        dsh = DataSetHelper()

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "pending"},
            "institutions": {"status": "succeeded"},
            "search": {"status": "succeeded"},
            "subjects": {"status": "succeeded"},
        }
        dsh.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertFalse(dsh.have_all_builds_succeeded())

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
        # Mock the CosmosClient and its methods
        mock_cosmos_client = mock.MagicMock()
        mock_database_client = mock.MagicMock()
        mock_container_client = mock.MagicMock()

        # Set up the mock chain
        mock_get_cosmos_client.return_value = mock_cosmos_client
        mock_cosmos_client.get_database_client.return_value = mock_database_client
        mock_database_client.get_container_client.return_value = mock_container_client

        dsh = DataSetHelper()

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "pending"},
            "institutions": {"status": "pending"},
            "search": {"status": "succeeded"},
            "subjects": {"status": "succeeded"},
        }
        dsh.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertFalse(dsh.have_all_builds_succeeded())

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
        # Mock the CosmosClient and its methods
        mock_cosmos_client = mock.MagicMock()
        mock_database_client = mock.MagicMock()
        mock_container_client = mock.MagicMock()

        # Set up the mock chain
        mock_get_cosmos_client.return_value = mock_cosmos_client
        mock_cosmos_client.get_database_client.return_value = mock_database_client
        mock_database_client.get_container_client.return_value = mock_container_client

        dsh = DataSetHelper()

        latest_dataset_doc = {}
        latest_dataset_doc["version"] = 3
        latest_dataset_doc["builds"] = {
            "courses": {"status": "succeeded"},
            "institutions": {"status": "succeeded"},
            "search": {"status": "succeeded"},
            "subjects": {"status": "succeeded"},
        }
        dsh.get_latest_doc = mock.MagicMock(return_value=latest_dataset_doc)
        self.assertTrue(dsh.have_all_builds_succeeded())


if __name__ == "__main__":
    unittest.main()