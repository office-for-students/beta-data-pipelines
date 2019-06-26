import datetime
import pprint
import uuid
import xml.etree.ElementTree as ET
import xmltodict

from ukrlp_client import UkrlpClient



# TODO move to common area as other functions use this
def get_uuid():
    id = uuid.uuid1()
    return str(id.hex)

def get_ukrlp_enrichment_data(ukprn):
    url = 'http://testws.ukrlp.co.uk:80/UkrlpProviderQueryWS5/ProviderQueryServiceV5'
    headers = {'Content-Type': 'text/xml'}

    soap_req = get_soap_req(ukprn)
    r = requests.post(url, data = soap_req, headers=headers)
    provider_contact = enrichment_data_dict['S:Envelope']['S:Body']['ns4:ProviderQueryResponse']['MatchingProviderRecords']['ProviderContent']
    build_lookup_entry(ukprn, provider_record)


def get_lookup_entry(ukprn, matching_provider_records):
    lookup_item = {}
    lookup_item['id'] = get_uuid()
    lookup_item['created_at'] = datetime.datetime.utcnow().isoformat()
    lookup_item['ukprn'] = ukprn
    lookup_item['ukprn_name'] = matching_provider_records['ProviderName']
    return lookup_item

def create_ukrlp_docs():
    """Parse HESA XML passed in and create JSON lookup table for  UKRLP data."""

    with open('../test-data/institutions.xml', 'r') as file:
        xml_string = file.read()

    root = ET.fromstring(xml_string)
    print(root)

    institution_count = 0
    for institution in root.iter('INSTITUTION'):
        raw_inst_data = xmltodict.parse(
            ET.tostring(institution))['INSTITUTION']
        ukprn = raw_inst_data['UKPRN']
        publicukprn = raw_inst_data['PUBUKPRN']

        # Get enrichment data from UKRLP
        matching_provider_records = UkrlpClient.get_matching_provider_records(ukprn)
        lookup_entry = get_lookup_entry(ukprn, matching_provider_records)

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(lookup_entry)

        institution_count+=1
        if institution_count >= 1:
            break


create_ukrlp_docs()
