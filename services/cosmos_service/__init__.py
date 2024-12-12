from services.cosmos_service.base import CosmosServiceBase
from services.utils import get_module_path_and_classname


def get_current_provider(
        provider_path: str,
        cosmos_database
) -> type["CosmosServiceBase"]:
    """
    Use this method when you need the provider for blob access.
    :param provider_path: eg "services.blob_service.BlobServiceLocal"
    :type provider_path: str
    :param cosmos_database: Cosmos database parameter for cosmos service object
    :type cosmos_database: 
    :return: Blob class as specified in environment variable (local or general)
    """
    module, class_name = get_module_path_and_classname(provider_path=provider_path)
    try:
        # initialises the class required from the module, passing in the init variables. Bish bash bosh.
        return getattr(module, class_name)(cosmos_database)
    except TypeError as e:
        raise TypeError(
            f"Unable to locate COSMOS_SERVICE_MODULE, COSMOS_SERVICE_MODULE must be sent in the environment :: {e}")
