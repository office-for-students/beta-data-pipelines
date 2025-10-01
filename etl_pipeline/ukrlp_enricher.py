from typing import Any
from typing import Dict
from typing import Type
from typing import Union

from etl_pipeline.utils import get_ukrlp_lookups


class UkRlpCourseEnricher:
    """Handles enriching courses with UKRLP data"""

    def __init__(self, cosmos_service: Type["CosmosService"], version: int) -> None:
        self.ukrlp_lookups = get_ukrlp_lookups(cosmos_service, version)

    def enrich_course(self, course: Dict[str, Any]) -> None:
        """
        Takes a course and enriches ukprn names with UKRLP data.

        :param course: Course data to enrich with UKRLP data
        :type course: Dict[str, Any]
        :return: None
        """
        ukprn = course["course"]["institution"]["ukprn"]
        course["course"]["institution"]["ukprn_name"] = self.get_ukprn_name(ukprn)
        course["course"]["institution"]["ukprn_welsh_name"] = self.get_ukprn_welsh_name(ukprn)

        pub_ukprn = course["course"]["institution"]["pub_ukprn"]
        course["course"]["institution"]["pub_ukprn_name"] = self.get_ukprn_name(pub_ukprn)
        course["course"]["institution"]["pub_ukprn_welsh_name"] = self.get_ukprn_welsh_name(pub_ukprn)
        course["course"]["institution"]["pub_ukprn_country"] = self.get_country(pub_ukprn)

    def get_ukprn_name(self, ukprn: str) -> str:
        """
        Takes a UKPRN lookup and returns an English name for the ukprn.

        :param ukprn: UKPRN lookup key
        :type ukprn: str
        :return: UKPRN name
        :rtype: str
        """
        if ukprn not in self.ukrlp_lookups:
            return "not available"

        return self.ukrlp_lookups[ukprn].get("ukprn_name", "not available")

    def get_ukprn_welsh_name(self, ukprn: str) -> str:
        """
        Takes a UKPRN lookup and returns a Welsh name for the ukprn.

        :param ukprn: UKPRN lookup key
        :type ukprn: str
        :return: UKPRN name
        :rtype: str
        """
        if ukprn not in self.ukrlp_lookups:
            return "not available"

        return self.ukrlp_lookups[ukprn].get("ukprn_welsh_name", "not available")

    def get_country(self, pub_ukprn: str) -> Union[Dict[str, str], str]:
        """
        Returns country data for the passed pub_ukprn's country

        :param pub_ukprn: UKPRN lookup key
        :type pub_ukprn: str
        :return: Corresponding country data
        :rtype: Union[Dict[str, str], str]
        """
        if pub_ukprn not in self.ukrlp_lookups:
            return "not available"

        return self.ukrlp_lookups[pub_ukprn].get("country")
