class CosmosDatabaseLocal:
    def __init__(self, database_name: str):
        self.database_name = database_name


class CosmosClientLocal:

    def __init__(self, url: str, credential: dict[str, str]):
        self.url = url
        self.credential = credential

    @staticmethod
    def get_database_client(database_name: str):
        return CosmosDatabaseLocal(database_name=database_name)
