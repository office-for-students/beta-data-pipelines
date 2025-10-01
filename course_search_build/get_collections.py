from typing import Any
from typing import Dict
from typing import List
from typing import Union


def get_collections_as_list(
        cosmos_container: Union[type['ContainerProxy'], type['ContainerLocal']],
        query: str,
) -> List[Dict[str, Any]]:
    """
    Takes a cosmos container and runs the passed query. Returns query result as a list.

    :param cosmos_container: Container to retrieve data from
    :type cosmos_container: Union[type['ContainerProxy'], type['ContainerLocal']]
    :param query: Query to run on container
    :type query: str
    :return: Query result as a list
    :rtype: List[Dict[str, Any]]
    """
    collections_list = list(cosmos_container.query_items(query=query, enable_cross_partition_query=True))

    return collections_list
