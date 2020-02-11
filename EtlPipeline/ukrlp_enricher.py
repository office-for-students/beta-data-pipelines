from SharedCode import utils


class UkRlpCourseEnricher:
    """Handles enriching courses with UKRLP data"""

    def __init__(self, version):
        self.ukrlp_lookups = utils.get_ukrlp_lookups(version)

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
        course["course"]["institution"][
            "pub_ukprn_welsh_name"
        ] = self.get_ukprn_welsh_name(pub_ukprn)

    def get_ukprn_name(self, ukprn):
        """Returns a name for the ukprn"""
        if ukprn not in self.ukrlp_lookups:
            return "not available"

        return self.ukrlp_lookups[ukprn].get("ukprn_name", "not available")

    def get_ukprn_welsh_name(self, ukprn):
        """Returns a name for the ukprn"""
        if ukprn not in self.ukrlp_lookups:
            return "not available"

        if not self.ukrlp_lookups[ukprn]["ukprn_welsh_name"]:
            return "not available"

        return self.ukrlp_lookups[ukprn].get("ukprn_welsh_name", "not available")
