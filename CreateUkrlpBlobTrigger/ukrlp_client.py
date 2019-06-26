import requests
import pprint
import xmltodict

class UkrlpClient:

    @staticmethod
    def get_soap_req(ukprn):
        """Creates and returns a SOAP request to query UKRLP for a ukprn entry"""

        soap_req = f"""
        <soapenv:Envelope
                xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:ukr="http://ukrlp.co.uk.server.ws.v3">
                <soapenv:Header/>
                <soapenv:Body>
                        <ukr:ProviderQueryRequest>
                                <SelectionCriteria>
                                        <UnitedKingdomProviderReferenceNumberList>
                                                <UnitedKingdomProviderReferenceNumber>{ukprn}</UnitedKingdomProviderReferenceNumber>
                                        </UnitedKingdomProviderReferenceNumberList>
                                        <CriteriaCondition>OR</CriteriaCondition>
                                        <StakeholderId>9</StakeholderId>
                                        <ApprovedProvidersOnly>No</ApprovedProvidersOnly>
                                        <ProviderStatus>A</ProviderStatus>
                                </SelectionCriteria>
                                <QueryId>9</QueryId>
                        </ukr:ProviderQueryRequest>
                </soapenv:Body>
        </soapenv:Envelope>
        """
        return soap_req


    @staticmethod
    def get_matching_provider_records(ukprn):
        url = 'http://testws.ukrlp.co.uk:80/UkrlpProviderQueryWS5/ProviderQueryServiceV5'
        headers = {'Content-Type': 'text/xml'}

        soap_req = UkrlpClient.get_soap_req(ukprn)
        r = requests.post(url, data = soap_req, headers=headers)
        #provider_contact = enrichment_data_dict['S:Envelope']['S:Body']['ns4:ProviderQueryResponse']['MatchingProviderRecords']['ProviderContent']
        r = requests.post(url, data=soap_req, headers=headers)
        status_code = r.status_code

        print(f"status code {status_code}")
        decoded_content = r.content.decode('utf-8')
        enrichment_data_dict = xmltodict.parse(decoded_content)
        provider_records = enrichment_data_dict['S:Envelope']['S:Body']['ns4:ProviderQueryResponse']['MatchingProviderRecords']

        pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(provider_records)
        return provider_records
        #enrichment_data_json = json.dumps(enrichment_data_dict)
