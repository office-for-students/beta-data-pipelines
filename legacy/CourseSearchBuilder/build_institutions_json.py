"""Module for creating the instituions.json file used by CMS"""

import io
import json
import re
from typing import Any
from typing import Dict
from typing import List

from decouple import config

from legacy.CourseSearchBuilder.get_collections import get_institutions
from legacy.services.blob import BlobService


def build_institutions_json_files() -> None:
    institution_list = get_institutions()

    generate_file(
        institution_list=institution_list,
        primary_name="pub_ukprn_welsh_name",
        secondary_name="pub_ukprn_name",
        first_trading_name="first_trading_name",
        legal_name="legal_name",
        other_names="other_names",
        blob_file="BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_CY"
    )
    generate_file(
        institution_list=institution_list,
        primary_name="pub_ukprn_name",
        secondary_name="pub_ukprn_welsh_name",
        first_trading_name="first_trading_name",
        legal_name="legal_name",
        other_names="other_names",
        blob_file="BLOB_INSTITUTIONS_JSON_FILE_BLOB_NAME_EN"
    )


def not_already_in_list(name: str, existing: List[str]) -> bool:
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
        blob_file: str
) -> None:
    blob_service = BlobService()
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

    storage_container_name = config("BLOB_JSON_FILES_CONTAINER_NAME")
    storage_blob_name = config(blob_file)
    blob_service.write_stream_file(storage_container_name, storage_blob_name, encoded_file)
    institutions_file.close()


def get_inst_entry(name: str, first_trading_name: str, legal_name: str, other_names: str) -> Dict[str, Any]:
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
    name = name.lower()
    name = remove_phrases_from_start(name)
    return name


def remove_phrases_from_start(name: str) -> str:
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
