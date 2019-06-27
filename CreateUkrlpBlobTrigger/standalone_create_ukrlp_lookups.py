import datetime
import json
import logging
import os
import pprint
import sys
import uuid
import xml.etree.ElementTree as ET
import xmltodict

from ukrlp_client import UkrlpClient
from ..SharedCode import utils

# TODO move to common area as other functions use this
def get_uuid():
    id = uuid.uuid1()
    return str(id.hex)

def get_address_entry(address):
    """Returns the address element"""
    address_item = {}

    address_from_keys = ('Address1', 'Address2', 'Address3', 'Address4', 'Town', 'County', 'PostCode')
    address_to_keys = ('line_1', 'line_2', 'line_3', 'line_4', 'town', 'county', 'post_code')

    for from_key, to_key in zip(address_from_keys, address_to_keys):
        if from_key in address:
            address_item[to_key] = address[from_key]

    return address_item


def get_contact_details(matching_provider_records):
    """Returns the contact details element"""

    contact_details = {}

    try:
        provider_contact = matching_provider_records['ProviderContact'][0]
    except KeyError:
        return contact_details

    address = provider_contact['ContactAddress']

    contact_details['address'] = get_address_entry(address)
    contact_details['telephone'] = provider_contact['ContactTelephone1']
    return contact_details


def get_lookup_entry(ukprn, matching_provider_records):
    """Returns the UKRLP lookup entry"""

    lookup_item = {}
    lookup_item['id'] = get_uuid()
    lookup_item['created_at'] = datetime.datetime.utcnow().isoformat()
    lookup_item['ukprn'] = ukprn
    lookup_item['ukprn_name'] = matching_provider_records['ProviderName']
    lookup_item['contact_details'] = get_contact_details(matching_provider_records)

    return lookup_item

def create_ukrlp_docs():
    """Parse HESA XML passed in and create JSON lookup table for  UKRLP data."""

    #with open('../../kis.xml', 'r') as file:
    with open('../test-data/institutions.xml', 'r') as file:
        xml_string = file.read()

    cosmosdb_client = utils.get_cosmos_client()

    # Get the relevant properties from Application Settings
    cosmosdb_database_id = os.environ['AzureCosmosDbDatabaseId']
    cosmosdb_collection_id = os.environ['AzureCosmosDbUkRlpCollectionId']

    # Define a link to the relevant CosmosDB Container/Document Collection
    collection_link = 'dbs/' + cosmosdb_database_id + \
        '/colls/' + cosmosdb_collection_id
    logging.info(f"collections_link {collection_link}")


    # Import the XML dataset
    root = ET.fromstring(xml_string)

    institution_count = 0
    failed_to_find_count = 0
    for institution in root.iter('INSTITUTION'):
        raw_inst_data = xmltodict.parse(
            ET.tostring(institution))['INSTITUTION']
        ukprn = raw_inst_data['UKPRN']
        print(f'next iteration {ukprn}')
        publicukprn = raw_inst_data['PUBUKPRN']

        matching_provider_records = UkrlpClient.get_matching_provider_records(ukprn)
        if not matching_provider_records:
            print(f'did not find record for {ukprn}')
            failed_to_find_count += 1
        else:
            lookup_entry = get_lookup_entry(ukprn, matching_provider_records)
            cosmosdb_client.CreateItem(collection_link, lookup_entry)
            institution_count+=1

    print(f'processed {institution_count} institutions')
    print(f'failed to processe {failed_to_find_count} institutions')
create_ukrlp_docs()
