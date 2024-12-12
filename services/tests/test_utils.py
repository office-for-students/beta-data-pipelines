# import unittest
# from unittest import mock
#
#
# class TestGetCollectionLink(unittest.TestCase):
#     def setUp(self):
#         self.db_id_env = "AzureCosmosDbDatabaseId"
#         self.collection_id_env = "AzureCosmosDbUkRlpCollection"
#         self.db_id = "test-db-id"
#         self.collection_id = "test-collection-id"
#
#     @mock.patch.dict(
#         "os.environ",
#         values={
#             "AzureCosmosDbDatabaseId": "test-db-id",
#             "AzureCosmosDbUkRlpCollection": "test-collection-id",
#         },
#         clear=True,
#     )
#     def test_get_collection_link(self):
#         expected_link = "dbs/" + self.db_id + "/colls/" + self.collection_id
#         collection_link = get_collection_link(
#             self.collection_id_env
#         )
#         self.assertEqual(collection_link, expected_link)
#
#
# if __name__ == "__main__":
#     unittest.main()
