"""Module for creating the institutions.json file used by CMS"""

import io
import json
import re
from typing import Any
from typing import Dict
from typing import List
from typing import TYPE_CHECKING

from constants import BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_CY
from constants import BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_EN
from constants import BLOB_JSON_FILES_CONTAINER_NAME

if TYPE_CHECKING:
    from legacy.services.blob import BlobService


def build_institutions_json_files(institution_list, blob_service: 'BlobService') -> None:
    """
    Retrieves a list of institutions and generates two JSON files, one for Welsh institution
    names and one for English. Files are stored as a blob and are not returned by this function.
    """
    generate_file(
        institution_list=institution_list,
        primary_name="pub_ukprn_welsh_name",
        secondary_name="pub_ukprn_name",
        first_trading_name="first_trading_name",
        legal_name="legal_name",
        other_names="other_names",
        blob_file=BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_CY,
        blob_service=blob_service
    )
    generate_file(
        institution_list=institution_list,
        primary_name="pub_ukprn_name",
        secondary_name="pub_ukprn_welsh_name",
        first_trading_name="first_trading_name",
        legal_name="legal_name",
        other_names="other_names",
        blob_file=BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_EN,
        blob_service=blob_service
    )


def not_already_in_list(name: str, existing: List[str]) -> bool:
    """
    Takes a name of an institution and a list of existing institution names.
    If the name is already in the list, it will be added to the list and this function returns True.
    Otherwise, it returns False.

    :param name: Name of institution to check
    :type name: str
    :param existing: List of existing institution names
    :type existing: List[str]
    :return: True if the name is already in the list, False otherwise
    :rtype: bool
    """
    if name not in existing:
        existing.append(name)
        return True
    return False


def generate_file(
        institution_list: List[Dict[str, Any]],
        primary_name: str,
        secondary_name: str,
        first_trading_name: str,
        legal_name: str,
        other_names: str,
        blob_file: str,
        blob_service: 'BlobService'
) -> None:
    """
    Takes parameters relating to the institution dataset and generates a JSON file using institution data
    from the passed institution list. The JSON file is saved to the blob referenced by the passed parameter
    and is not returned by this function.

    :param institution_list: List of dictionaries containing institution data
    :type institution_list: List[Dict[str, Any]]
    :param primary_name: Key for the institution's primary name
    :type primary_name: str
    :param secondary_name: Key for the institution's secondary name
    :type secondary_name: str
    :param first_trading_name: Key for the institution's first trading name
    :type first_trading_name: str
    :param legal_name: Key for the institution's legal name
    :type legal_name: str
    :param other_names: Key for the institution's other names
    :type other_names: str
    :param blob_file: Path to the blob for the generated file to be stored
    :type blob_file: str
    :param blob_service: Blob service object used to write file
    :type blob_service: BlobService
    :return: None
    """
    institutions_file = io.StringIO()
    institutions = []
    for val in institution_list:
        institution = val["institution"]
        primary = institution[primary_name]
        secondary = institution[secondary_name]
        first_trading = institution.get(first_trading_name, "")
        legal = institution.get(legal_name, "")
        other = institution.get(other_names, "")

        if isinstance(primary, str):
            inst_entry = get_inst_entry(primary, first_trading, legal, other)
            institutions.append(inst_entry)
        elif isinstance(secondary, str):
            inst_entry = get_inst_entry(secondary, first_trading, legal, other)
            institutions.append(inst_entry)

    institutions.sort(key=lambda x: x["order_by_name"])

    de_duped = []
    final = [name for name in institutions if not_already_in_list(name=name["name"], existing=de_duped)]

    json.dump(final, institutions_file, indent=4)
    encoded_file = institutions_file.getvalue().encode('utf-8')

    blob_service.write_stream_file(BLOB_JSON_FILES_CONTAINER_NAME, blob_file, encoded_file)
    institutions_file.close()


def get_inst_entry(name: str, first_trading_name: str, legal_name: str, other_names: str) -> Dict[str, Any]:
    """
    Takes relevant institution parameters and returns a dictionary containing these parameters.
    Also includes the institution's name without any prefixes and its first letter.

    :param name: Name of institution
    :type name: str
    :param first_trading_name: First trading name of institution
    :type first_trading_name: str
    :param legal_name: Legal name of institution
    :type legal_name: str
    :param other_names: Any other names of the institution
    :type other_names: str
    :return: Dictionary containing all relevant parameters for the institution
    :rtype: Dict[str, Any]
    """
    order_by_name = get_order_by_name(name)
    alphabet = order_by_name[0]
    entry = {
        "alphabet": alphabet,
        "name": name,
        "first_trading_name": first_trading_name,
        "legal_name": legal_name,
        "other_names": other_names,
        "order_by_name": order_by_name
    }
    return entry


def get_order_by_name(name: str) -> str:
    """
    Takes an institution name and removes prefixes then returns the raw institution name in lowercase.

    :param name: Name of institution
    :type name: str
    :return: Lowercase institution name without any prefixes
    :rtype: str
    """
    name = name.lower()
    name = remove_phrases_from_start(name)
    return name


def remove_phrases_from_start(name: str) -> str:
    """
    Takes an institution name and removes any prefixes (e.g., "The university of", etc.), then returns
    the raw institution name.

    :param name: Institution name from which to remove prefixes
    :type name: str
    :return: Institution name without prefixes
    :rtype: str
    """
    if re.search(r"^the university of ", name):
        name = re.sub(r"^the university of ", "", name)
    if re.search(r"^university of ", name):
        name = re.sub(r"^university of ", "", name)
    if re.search(r"^university for ", name):
        name = re.sub(r"^university for ", "", name)
    if re.search(r"^the ", name):
        name = re.sub(r"^the ", "", name)
    if re.search(r"^prifysgol ", name):
        name = re.sub(r"^prifysgol ", "", name)
    if re.search(r"^coleg ", name):
        name = re.sub(r"^coleg ", "", name)
    if re.search(r"^y coleg ", name):
        name = re.sub(r"^y coleg ", "", name)
    if re.search(r"^y brifysgol ", name):
        name = re.sub(r"^y brifysgol ", "", name)
    name = name.strip()
    name = name.replace(",", "")
    return name
