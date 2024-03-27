import importlib
from typing import Any
from typing import Tuple
from typing import Type


def get_current_provider(
        provider_path: str,
        service_string: str = None
) -> Type["BlobServiceBase"]:
    """
    Use this method when you need the provider for blob access.
    :param provider_path: eg "services.blob_service.BlobServiceLocal"
    :return:
    """
    module, class_name = _get_module_path_and_classname(provider_path=provider_path)
    try:
        # initialises the class required from the module, passing in the init variables. Bish bash bosh.
        return getattr(module, class_name)(service_string)
    except TypeError as e:
        raise TypeError(
            f"Unable to locate BLOB_SERVICE_MODULE, BLOB_SERVICE_MODULE must be sent in the environment :: {e}")


def _get_module_path_and_classname(provider_path: str) -> Tuple[Any, str]:
    module_string = provider_path
    # split by the `.`
    module_path_list = module_string.split(".")
    # get the last item, for the classname
    class_name = module_path_list.pop()
    # recombine the rest of the array for the module path
    module_import = '.'.join(module_path_list)
    # importlib lets you dynamically import the correct file
    module = importlib.import_module(module_import)

    return module, class_name
