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
        mock_get_cosmos_client.return_value = mock.MagicMock()
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
        dsh = DataSetHelper()
        dsh.cosmos_client.UpsertItem = mock.MagicMock()
        dsh.get_latest_doc = mock.MagicMock()
        dsh.update_status("courses", "in progress")
        dsh.get_latest_doc.assert_called_once()
        dsh.cosmos_client.UpsertItem.assert_called_once()


if __name__ == "__main__":
    unittest.main()
