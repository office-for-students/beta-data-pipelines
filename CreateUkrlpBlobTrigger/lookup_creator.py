"""Contains the class responsibe for creating UKRLP lookups"""

import datetime
import logging
import xml.etree.ElementTree as ET

import xmltodict

from SharedCode import utils

from .ukrlp_client import UkrlpClient


class LookupCreator:
    """Creates lookups for UKRLP data in Cosmos DB"""

    def __init__(self, xml_string):
        self.cosmosdb_client = utils.get_cosmos_client()
        self.xml_string = xml_string
        self.lookups_created = []
        self.ukrlp_no_info_list = []
        self.db_entries_list = []
        self.collection_link = utils.get_collection_link(
            'AzureCosmosDbDatabaseId', 'AzureCosmosDbUkRlpCollectionId')

    @staticmethod
    def get_address(address):
        """Returns the address element"""

        address_item = {}

        address_from_keys = ('Address1', 'Address2', 'Address3', 'Address4',
                             'Town', 'County', 'PostCode')
        address_to_keys = ('line_1', 'line_2', 'line_3', 'line_4', 'town',
                           'county', 'post_code')

        for from_key, to_key in zip(address_from_keys, address_to_keys):
            if from_key in address:
                address_item[to_key] = address[from_key]

        return address_item

    @staticmethod
    def get_contact_details(ukprn, matching_provider_records):
        """Returns the contact details element"""

        contact_details = {}

        try:
            provider_contact = matching_provider_records['ProviderContact'][0]
        except KeyError:
            logging.error(f'No ProviderContact from UKRLP for {ukprn}')
            return contact_details

        address = provider_contact['ContactAddress']

        contact_details['address'] = LookupCreator.get_address(address)
        contact_details['telephone'] = provider_contact['ContactTelephone1']
        return contact_details

    @staticmethod
    def get_lookup_entry(ukprn, matching_provider_records):
        """Returns the UKRLP lookup entry"""

        lookup_item = {}
        lookup_item['id'] = utils.get_uuid()
        lookup_item['created_at'] = datetime.datetime.utcnow().isoformat()
        lookup_item['ukprn'] = ukprn
        lookup_item['ukprn_name'] = matching_provider_records['ProviderName']
        lookup_item['contact_details'] = LookupCreator.get_contact_details(ukprn,
            matching_provider_records)

        return lookup_item

    def entry_exists(self, ukprn):
        """Check if the entry for a ukprn exists Cosmos DB"""

        if ukprn in self.db_entries_list:
            # This ukprn is already in the database so no need to query
            logging.debug(f'{ukprn} is in the DB so no need to query')
            return True

        query = {'query': f"SELECT * FROM c where c.ukprn = '{ukprn}'"}
        logging.debug(f'query: {query}')

        options = {}
        options['enableCrossPartitionQuery'] = True

        res = list(
            self.cosmosdb_client.QueryItems(self.collection_link, query,
                                            options))
        if not res:
            return False

        logging.debug(f'Append {ukprn} to the list {self.db_entries_list}')
        self.db_entries_list.append(ukprn)
        return True

    def create_ukrlp_lookup(self, ukprn):
        """Get the UKRLP record, transform it, and write it to Cosmos DB"""

        matching_provider_records = UkrlpClient.get_matching_provider_records(
            ukprn)

        if not matching_provider_records:
            logging.error(f'UKRLP did not return the data for {ukprn}')
            if ukprn not in self.ukrlp_no_info_list:
                self.ukrlp_no_info_list.append(ukprn)
            return False

        lookup_entry = LookupCreator.get_lookup_entry(
            ukprn, matching_provider_records)

        self.cosmosdb_client.CreateItem(self.collection_link, lookup_entry)
        return True

    def create_ukrlp_lookups(self):
        """Parse HESA XML and create JSON lookup table for UKRLP data."""

        root = ET.fromstring(self.xml_string)

        for institution in root.iter('INSTITUTION'):
            raw_inst_data = xmltodict.parse(
                ET.tostring(institution))['INSTITUTION']
            ukprn = raw_inst_data.get('UKPRN')
            pubukprn = raw_inst_data.get('PUBUKPRN')

            if ukprn and not self.entry_exists(ukprn):
                if self.create_ukrlp_lookup(ukprn):
                    self.lookups_created.append(ukprn)

            if pubukprn and not self.entry_exists(pubukprn):
                if self.create_ukrlp_lookup(pubukprn):
                    self.lookups_created.append(ukprn)

        logging.info(f'lookups_created = {len(self.lookups_created)}')

        if self.ukrlp_no_info_list:
            logging.info(
                f'UKRLP did not return info for the following {len(self.ukrlp_no_info_list)} ukprn(s)'
            )
            for ukprn in self.ukrlp_no_info_list:
                logging.info(f'{ukprn}')

        logging.info(
            f'DB entries existed for {len(self.db_entries_list)} ukprns tried')
