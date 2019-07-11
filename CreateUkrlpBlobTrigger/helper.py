class Helper:
    """Responsible for aquiring the correct provider name from the data returned by ukrlp API"""

    @staticmethod
    def get_list(term):
        """Returns a list of terms"""

        if isinstance(term, dict):
            return [term]
        
        return term

    @staticmethod
    def get_provider_name(matching_provider_records):
        """Returns a single name for the provider"""

        # Get provider name if alias does not exist in ukrlp response body
        if 'ProviderAliases' in matching_provider_records and matching_provider_records['ProviderAliases'] is not None:
            aliases = Helper.get_list(matching_provider_records['ProviderAliases'])
            return aliases[0]['ProviderAlias']
        
        return matching_provider_records['ProviderName']
