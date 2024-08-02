from typing import Type

from services.utils import get_module_path_and_classname


def get_current_provider(
        provider_path: str,
        service_string: str = None
) -> Type["BlobServiceBase"]:
    """
    Use this method when you need the provider for blob access.
    :param provider_path: eg "services.blob_service.BlobServiceLocal"
    :type provider_path: str
    :param service_string: Service string parameter for blob object
    :type service_string: str
    :return: Blob class as specified in environment variable (local or general)
    """
    module, class_name = get_module_path_and_classname(provider_path=provider_path)
    try:
        # initialises the class required from the module, passing in the init variables. Bish bash bosh.
        return getattr(module, class_name)(service_string)
    except TypeError as e:
        raise TypeError(
            f"Unable to locate BLOB_SERVICE_MODULE, BLOB_SERVICE_MODULE must be sent in the environment :: {e}")
