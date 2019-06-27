import requests
import pprint
import xmltodict
import os

class UkrlpClient:
    """ Responsible for requesting data from UKRLP and returning data"""

    @staticmethod
    def get_soap_req(ukprn):
        """Returns a SOAP request to query a specific UKRLP for a ukprn entry"""

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

        url = os.environ['UkRlpUrl']

        headers = {'Content-Type': 'text/xml'}

        soap_req = UkrlpClient.get_soap_req(ukprn)
        r = requests.post(url, data=soap_req, headers=headers)
        status_code = r.status_code
        if status_code != 200:
            return None

        decoded_content = r.content.decode('utf-8')
        enrichment_data_dict = xmltodict.parse(decoded_content)

        try:
            provider_records = enrichment_data_dict['S:Envelope']['S:Body']['ns4:ProviderQueryResponse']['MatchingProviderRecords']
        except KeyError:
            return None


        return provider_records
