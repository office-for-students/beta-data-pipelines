from __app__.SharedCode import utils


class UkRlpCourseEnricher:
    """Handles enriching courses with UKRLP data"""

    def __init__(self):
        self.ukrlp_lookups = utils.get_ukrlp_lookups()

    def enrich_course(self, course):
        """Takes a course and enriches ukprn names with UKRLP data"""

        ukprn = course["course"]["institution"]["ukprn"]
        course["course"]["institution"]["ukprn_name"] = self.get_ukprn_name(
            ukprn
        )

        pub_ukprn = course["course"]["institution"]["pub_ukprn"]
        course["course"]["institution"][
            "pub_ukprn_name"
        ] = self.get_ukprn_name(pub_ukprn)

    def get_ukprn_name(self, ukprn):
        """Returns a name for the ukprn"""
        if ukprn not in self.ukrlp_lookups:
            return "not availble"

        return self.ukrlp_lookups[ukprn].get("ukprn_name", "not available")
