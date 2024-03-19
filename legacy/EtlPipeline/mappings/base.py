from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from legacy.EtlPipeline.subject_enricher import SubjectCourseEnricher
from legacy.EtlPipeline.utils import get_subject


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

    def __init__(self, mapping_id: str, subject_enricher: SubjectCourseEnricher):
        if mapping_id not in self.OPTIONS:
            raise InvalidMappingId(f"Invalid mapping_id {mapping_id}for {self}")

        self.mapping_id = mapping_id
        self.subject_enricher = subject_enricher

    def get_mappings(self) -> List[Tuple[str, str]]:
        raise NotImplemented

    def map_xml_to_json_array(
            self,
            xml_as_array: List[Dict[str, Any]],
            mappings: Optional[List[Tuple[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Takes an XML as a list and optionally a list of mappings, and returns a list of JSON objects for each
        dictionary in the passed XML as array. Optionally takes mappings to override the defaults.

        :param xml_as_array: XML element to convert to a JSON list
        :type xml_as_array: List[Dict[str, Any]]
        :param mappings: Optional, if provided will overwrite the default mappings
        :type mappings: Optional[List[Tuple[str, str]]]
        :return: List of JSON dictionaries
        :rtype: List[Dict[str, Any]]
        """
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
                            json_data[json_key] = elem.get(xml_key)
                            self.custom_unavailable(json_data=json_data, elem=elem, key=json_key)

                        elif json_key == "subject":
                            json_data[json_key] = get_subject(elem.get(xml_key), self.subject_enricher)

                        else:
                            json_data[json_key] = elem.get(xml_key)

                self.per_course_unavailable(json_data=json_data)

                self.final_unavailable(json_data)

                json_array.append(json_data)

        return json_array

    def per_course_unavailable(self, json_data: Dict[str, Any]) -> None:
        pass

    def custom_unavailable(self, json_data: Dict[str, Any], elem: Dict[str, Any], key: str) -> None:
        # Required if you have set unavailable_keys and that key is found during mapping
        raise NotImplemented

    def final_unavailable(self, json_data: Dict[str, Any]) -> None:
        # optional
        pass

    @staticmethod
    def in_and_not_na(key: str, data: Dict[str, Any]) -> bool:
        """
        Takes a key and a data dictionary, and checks if the corresponding value exists and is not "na".
        Returns True if both of these conditions are met, otherwise False.

        :param key: Key to lookup data dictionary
        :type key: str
        :param data: Data dictionary to check value of
        :type data: Dict[str, Any]
        :return: True if the value exists and is not "na", otherwise False
        :rtype: bool
        """
        if key in data and data.get(key).lower() != "na":
            return True
        return False
