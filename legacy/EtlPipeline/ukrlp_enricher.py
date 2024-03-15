from typing import Any
from typing import Dict
from typing import Union

from legacy.services import utils


class UkRlpCourseEnricher:
    """Handles enriching courses with UKRLP data"""

    def __init__(self, version: str) -> None:
        self.ukrlp_lookups = utils.get_ukrlp_lookups(version)

    def enrich_course(self, course: Dict[str, Any]) -> None:
        """Takes a course and enriches ukprn names with UKRLP data"""

        ukprn = course["course"]["institution"]["ukprn"]
        course["course"]["institution"]["ukprn_name"] = self.get_ukprn_name(
            ukprn
        )
        course["course"]["institution"]["ukprn_welsh_name"] = self.get_ukprn_welsh_name(
            ukprn
        )

        pub_ukprn = course["course"]["institution"]["pub_ukprn"]
        course["course"]["institution"][
            "pub_ukprn_name"
        ] = self.get_ukprn_name(pub_ukprn)
        course["course"]["institution"][
            "pub_ukprn_welsh_name"
        ] = self.get_ukprn_welsh_name(pub_ukprn)
        course["course"]["institution"]["pub_ukprn_country"] = self.get_country(pub_ukprn)

    def get_ukprn_name(self, ukprn: str) -> str:
        """Returns a name for the ukprn"""
        if ukprn not in self.ukrlp_lookups:
            return "not available"

        return self.ukrlp_lookups[ukprn].get("ukprn_name", "not available")

    def get_ukprn_welsh_name(self, ukprn: str) -> str:
        """Returns a name for the ukprn"""
        if ukprn not in self.ukrlp_lookups:
            return "not available"

        return self.ukrlp_lookups[ukprn].get("ukprn_welsh_name", "not available")

    def get_country(self, pub_ukprn: str) -> Union[Dict[str, str], str]:
        """Returns a {code: code, "name": name} for the pub_ukprn country"""
        if pub_ukprn not in self.ukrlp_lookups:
            return "not available"

        return self.ukrlp_lookups[pub_ukprn].get("country")
