import unittest
from unittest import mock

# import SharedCode
from SharedCode.dataset_helper import DataSetHelper


class TestGetCollectionLink(unittest.TestCase):
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
        try:
            dsh = DataSetHelper()
        except:
            self.fail(
                "DataSetHelper initialisation raised unexpected Exception"
            )


if __name__ == "__main__":
    unittest.main()
