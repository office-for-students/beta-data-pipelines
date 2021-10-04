from collections import Callable
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from EtlPipeline.utils import get_subject


class InvalidMappingId(Exception):
    pass


class BaseMappings:
    """
    Base Mappings:

    Subclass to process mappings for course data (LEO3, LEO5, GO)
    OPTIONS should specify what mapping_ids are accepted by this class
    and get_mappings should return an array of tuples (str, str).
    """
    OPTIONS = []
    unavailable_keys = []
    unavailable_method = None

    def __init__(self, mapping_id):
        if mapping_id not in self.OPTIONS:
            raise InvalidMappingId(f"Invalid mapping_id {mapping_id}for {self}")

        self.mapping_id = mapping_id

    def get_mappings(self) -> List[Tuple[str, str]]:
        raise NotImplemented

    def map_xml_to_json_array(
            self,
            xml_as_array: List[Any],
            mappings: Optional[List[Tuple[str, str]]] = None,
            unavailable_messages: Optional[List[Tuple[Tuple[str, str], Tuple[str, str, str]]]] = None,
    ) -> List[Dict[str, Any]]:
        json_array = []
        # Can overwrite mappings if needed, otherwise the default is get_mappings()
        if not mappings:
            mappings = self.get_mappings()

        if xml_as_array and len(xml_as_array) > 0:
            for elem in xml_as_array:
                json_data = {}
                for xml_key, json_key in mappings:

                    # If key isn't there or it's NA (should be NA as of September 2021)
                    if self.in_and_not_na(key=xml_key, data=elem):

                        if json_key in self.unavailable_keys:
                            json_data[json_key] = elem[xml_key]
                            self.custom_unavailable(json_data=json_data, elem=elem, key=json_key)

                        elif json_key == "subject":
                            json_data[json_key] = get_subject(elem[xml_key])

                        else:
                            json_data[json_key] = elem[xml_key]

                if unavailable_messages:
                    for unavailable in unavailable_messages:
                        self.modify_sector_with_unavailable(json_data, unavailable, self.unavailable_method)

                self.final_unavailable(json_data)

                json_array.append(json_data)

        return json_array

    def custom_unavailable(self, json_data: Dict[str, Any], elem: List, key: str) -> None:
        # Required if you have set unavailable_keys and that key is found during mapping
        raise NotImplemented

    def final_unavailable(self, json_data):
        # optional
        pass

    @staticmethod
    def modify_sector_with_unavailable(
            json_data: Dict[str, str],
            unavailable: Tuple[Tuple[str, str], Tuple[str, str, str]],
            method=Callable) -> None:

        keys = unavailable[0]
        print(f"keys = {keys}")
        print(f"unavailable[1] {unavailable[1]}")
        print(f" method {method}")

        json_data[keys[0]], json_data[keys[1]] = method()

    @staticmethod
    def in_and_not_na(key: str, data: Dict[str, Any]) -> bool:
        if key in data:
            if data[key].lower() != "na":
                return True
        return False
