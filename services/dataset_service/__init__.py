from typing import Union

from azure.cosmos import ContainerProxy

from services.cosmos_service.local import ContainerLocal
from services.dataset_service.base import DataSetServiceBase
from services.utils import get_module_path_and_classname


def get_current_provider(
        provider_path: str,
        cosmos_container: Union[ContainerProxy, ContainerLocal]
) -> type["DataSetServiceBase"]:
    """
    Use this method when you need the provider for blob access.
    :param provider_path: eg "services.blob_service.BlobServiceLocal"
    :type provider_path: str
    :param cosmos_container: Cosmos container for data set service object
    :type cosmos_container: ContainerProxy
    :return: Blob class as specified in environment variable (local or general)
    """
    module, class_name = get_module_path_and_classname(provider_path=provider_path)
    try:
        # initialises the class required from the module, passing in the init variables. Bish bash bosh.
        service = getattr(module, class_name)(cosmos_container)
        return service
    except TypeError as e:
        raise TypeError(
            f"Unable to locate DATA_SET_SERVICE_MODULE, DATA_SET_SERVICE_MODULE must be sent in the environment :: {e}")
