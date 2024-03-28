from typing import TYPE_CHECKING
from typing import Union

from azure.cosmos import CosmosClient

from services.utils import get_module_path_and_classname


def get_current_provider(
        provider_path: str,
        url: str,
        credential: dict[str, str]
) -> Union[type["CosmosServiceBase", CosmosClient]]:
    """
    Use this method when you need the provider for blob access.
    :param provider_path: eg "services.blob_service.BlobServiceLocal"
    :type provider_path: str
    :param url: URL parameter for cosmos client
    :type url: str
    :param credential: Credentials for cosmos client
    :type credential: dict[str, str]
    :return: Blob class as specified in environment variable (local or general)
    """
    module, class_name = get_module_path_and_classname(provider_path=provider_path)
    try:
        # initialises the class required from the module, passing in the init variables. Bish bash bosh.
        return getattr(module, class_name)(url=url, credential=credential)
    except TypeError as e:
        raise TypeError(
            f"Unable to locate COSMOS_CLIENT_MODULE, COSMOS_CLIENT_MODULE must be sent in the environment :: {e}")
