"""Contains the class responsibe for creating UKRLP lookups"""

import datetime
import inspect
import logging
import os
import sys
import time
import defusedxml.ElementTree as ET

import xmltodict

from SharedCode.dataset_helper import DataSetHelper


# TODO investigate setting PATH in Azure so can remove this
CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from SharedCode.utils import get_collection_link, get_cosmos_client, get_uuid

from ukrlp_client import UkrlpClient
from helper import Helper


class LookupCreator:
    """Creates lookups for UKRLP data in Cosmos DB"""

    def __init__(self, xml_string):
        self.cosmosdb_client = get_cosmos_client()
        self.dsh = DataSetHelper()
        self.xml_string = xml_string
        self.lookups_created = []
        self.ukrlp_no_info_list = []
        self.db_entries_list = []
        self.collection_link = get_collection_link(
            "AzureCosmosDbDatabaseId", "AzureCosmosDbUkRlpCollectionId"
        )
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__))
        )
        with open(
            os.path.join(__location__, "institution_whitelist.txt")
        ) as f:
            institutions_whitelist = f.readlines()
            self.institutions_whitelist = [
                institution.strip() for institution in institutions_whitelist
            ]

    def create_ukrlp_lookups(self):
        """Parse HESA XML and create JSON lookup table for UKRLP data."""
        root = ET.fromstring(self.xml_string)
        
        options = {"partitionKey": str(self.dsh.get_latest_version_number())}
        sproc_link = self.collection_link + "/sprocs/bulkImport"

        new_docs = []
        sproc_count = 0

        for institution in root.iter("INSTITUTION"):
            raw_inst_data = xmltodict.parse(ET.tostring(institution))[
                "INSTITUTION"
            ]
            ukprn = raw_inst_data.get("UKPRN")
            pubukprn = raw_inst_data.get("PUBUKPRN")

            if ukprn and not self.entry_exists(ukprn):
                result, ukrlp_doc = self.create_ukrlp_lookup(ukprn)

                if result:
                    self.lookups_created.append(ukprn)
                    new_docs.append(ukrlp_doc)
                    sproc_count += 1

            if pubukprn and not self.entry_exists(pubukprn):
                result, ukrlp_doc = self.create_ukrlp_lookup(pubukprn)

                if result:
                    self.lookups_created.append(pubukprn)
                    new_docs.append(ukrlp_doc)
                    sproc_count += 1

            if sproc_count >= 100:
                logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
                self.cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
                logging.info(f"Successfully loaded another {sproc_count} documents")
                # Reset values
                new_docs = []
                sproc_count = 0
                time.sleep(2)

        if sproc_count > 0:
            logging.info(f"Begining execution of stored procedure for {sproc_count} documents")
            self.cosmosdb_client.ExecuteStoredProcedure(sproc_link, [new_docs], options)
            logging.info(f"Successfully loaded another {sproc_count} documents")
            # Reset values
            new_docs = []
            sproc_count = 0

        logging.info(f"lookups_created = {len(self.lookups_created)}")

        if self.ukrlp_no_info_list:
            logging.info(
                f"UKRLP did not return info for the following "
                f"{len(self.ukrlp_no_info_list)} ukprn(s)"
            )
            for ukprn in self.ukrlp_no_info_list:
                logging.info(f"{ukprn}")

        logging.info(
            f"DB entries existed for {len(self.db_entries_list)} ukprns tried"
        )

    def entry_exists(self, ukprn):
        """Check if the entry for a ukprn exists Cosmos DB"""

        if ukprn in self.db_entries_list:
            # This ukprn is already in the database so no need to query
            logging.debug(f"{ukprn} is in the DB so no need to query")
            return True

        query = {"query": f"SELECT * FROM c where c.ukprn = '{ukprn}'"}
        logging.debug(f"query: {query}")

        options = {}
        options["enableCrossPartitionQuery"] = True

        res = list(
            self.cosmosdb_client.QueryItems(
                self.collection_link, query, options
            )
        )
        if not res:
            return False

        logging.debug(f"Append {ukprn} to the list {self.db_entries_list}")
        self.db_entries_list.append(ukprn)
        return True

    def create_ukrlp_lookup(self, ukprn):
        """Get the UKRLP record, transform it, and write it to Cosmos DB"""

        matching_provider_records = UkrlpClient.get_matching_provider_records(
            ukprn
        )

        if not matching_provider_records:
            logging.error(f"UKRLP did not return the data for {ukprn}")
            if ukprn not in self.ukrlp_no_info_list:
                self.ukrlp_no_info_list.append(ukprn)
            return False, None

        lookup_entry = self.get_lookup_entry(ukprn, matching_provider_records)

        return True, lookup_entry

    def get_lookup_entry(self, ukprn, matching_provider_records):
        """Returns the UKRLP lookup entry"""

        lookup_item = {}
        lookup_item["id"] = get_uuid()
        lookup_item["created_at"] = datetime.datetime.utcnow().isoformat()
        lookup_item["ukprn"] = ukprn
        lookup_item["partition_key"] = str(self.dsh.get_latest_version_number())

        provider_name = Helper.get_provider_name(matching_provider_records)
        if self.title_case_needed(provider_name):
            provider_name = LookupCreator.title_case(provider_name)
        lookup_item["ukprn_name"] = provider_name

        lookup_item["contact_details"] = LookupCreator.get_contact_details(
            ukprn, matching_provider_records
        )

        return lookup_item

    def title_case_needed(self, name):
        if name not in self.institutions_whitelist:
            return True
        return False

    @staticmethod
    def title_case(s):
        exceptions = ["an", "and", "for", "in", "of", "the"]
        s = s.lower()
        word_list = s.split()
        result = [word_list[0].capitalize()]
        for word in word_list[1:]:
            result.append(word if word in exceptions else word.capitalize())
        return " ".join(result)

    @staticmethod
    def get_contact_details(ukprn, matching_provider_records):
        """Returns the contact details element"""

        contact_details = {}

        provider_contact = None
        contacts = Helper.get_list(
            matching_provider_records["ProviderContact"]
        )
        try:
            for contact in contacts:
                if contact["ContactType"] == "L" and contact["ContactAddress"]:
                    provider_contact = contact
                if contact["ContactType"] == "P" and contact["ContactAddress"]:
                    provider_contact = contact
                    break

        except KeyError:
            logging.error(f"No ProviderContact from UKRLP for {ukprn}")
            return contact_details

        if not provider_contact:
            logging.error(
                f"No ProviderContact with type L or P from UKRLP for {ukprn}"
            )
            return contact_details

        if provider_contact["ContactType"] == "L":
            logging.info(f"Using legal address for this {ukprn}")

        address = provider_contact["ContactAddress"]

        contact_details["address"] = LookupCreator.get_address(address)
        contact_details["telephone"] = provider_contact["ContactTelephone1"]
        contact_details["website"] = LookupCreator.get_website(
            matching_provider_records
        )
        return contact_details

    @staticmethod
    def get_address(address):
        """Returns the address element"""

        address_item = {}

        address_from_keys = (
            "Address1",
            "Address2",
            "Address3",
            "Address4",
            "Town",
            "County",
            "PostCode",
        )
        address_to_keys = (
            "line_1",
            "line_2",
            "line_3",
            "line_4",
            "town",
            "county",
            "post_code",
        )

        for from_key, to_key in zip(address_from_keys, address_to_keys):
            if from_key in address:
                address_item[to_key] = address[from_key]

        return address_item

    @staticmethod
    def get_website(matching_provider_records):

        website = "No website available"
        for i, provider_contact in enumerate(
            matching_provider_records["ProviderContact"]
        ):
            if i > 1:
                # Currently, we look in the first two Provider Contacts
                break

            if "ContactWebsiteAddress" in provider_contact:
                website = provider_contact["ContactWebsiteAddress"]
                break
        return website
