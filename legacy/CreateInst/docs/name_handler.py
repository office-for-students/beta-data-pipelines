import csv
import logging
import re
from typing import List


class InstitutionProviderNameHandler:
    def __init__(self, white_list: List[str], welsh_uni_names: List[str]) -> None:
        self.white_list = white_list
        self.welsh_uni_names = welsh_uni_names

    @staticmethod
    def title_case(s: str) -> str:
        """
        Converts the passed string to title case

        :param s: String to convert to title case
        :type s: str
        :return: Title case converted string
        :rtype: str
        """
        s = re.sub(
            r"[A-Za-z]+('[A-Za-z]+)?",
            lambda word: word.group(0).capitalize(),
            s
        )

        exclusions = ["an", "and", "for", "in", "of", "the"]
        word_list = s.split()
        result = [word_list[0]]
        for word in word_list[1:]:
            result.append(word.lower() if word.lower() in exclusions else word)

        s = " ".join(result)

        return s

    def get_welsh_uni_name(self, pub_ukprn: str, provider_name: str) -> str:
        """
        Reads CSV of Welsh institution names and checks if the pub_ukprn is found. If it is, return the corresponding
        Welsh institution name, otherwise returns the passed provider name.

        :param pub_ukprn: Code of institution
        :type pub_ukprn: str
        :param provider_name: Provider name to return as a default
        :type provider_name: str
        :return: Welsh institution name if found, otherwise the passed provider name
        :rtype: str
        """
        rows = csv.reader(self.welsh_uni_names)
        for row in rows:
            if row[0] == pub_ukprn:
                logging.info(f"Found welsh name for {pub_ukprn}")
                return row[1]
        return provider_name

    def should_edit_title(self, title: str) -> bool:
        """
        Checks the whitelist of institutions and converts the passed institution title to title case if it is not found.

        :param title: Institution title to check
        :type title: str
        :return: True if the name is not in the whitelist (i.e., it should be edited), else False
        """
        if title not in self.white_list:
            return True
        return False

    def presentable(self, provider_name: str) -> str:
        """
        Takes a provider name and converts to title case if necessary

        :param provider_name: Provider name to check
        :type provider_name: str
        :return: Presentable provider name
        :rtype: str
        """
        if self.should_edit_title(provider_name):
            provider_name = self.title_case(provider_name)
        return provider_name
