from typing import Any
from typing import Dict
from typing import List

from services.utils import get_module_path_and_classname


def get_current_provider(
        provider_path: str,
        url: str,
        api_key: str,
        api_version: str,
        version: str,
        docs: List[Dict[str, Any]]
) -> type["SearchServiceBase"]:
    """
    Use this method when you need the provider for blob access.
    :param provider_path: eg "services.blob_service.BlobServiceLocal"
    :type provider_path: str
    :param url: URL of search index
    :type url: str
    :param api_key: API key for search API
    :type api_key: str
    :param api_version: Version of the search API
    :type api_version: str
    :param version: Version of the dataset
    :type version: int
    :param docs: List of course data
    :type docs: List[Dict[str, Any]]
    :return: Search service class as specified in environment variable (local or general)
    """
    module, class_name = get_module_path_and_classname(provider_path=provider_path)
    try:
        # initialises the class required from the module, passing in the init variables. Bish bash bosh.
        return getattr(module, class_name)(url, api_key, api_version, version, docs)
    except TypeError as e:
        raise TypeError(
            f"Unable to locate SEARCH_SERVICE_MODULE, SEARCH_SERVICE_MODULE must be sent in the environment :: {e}")
