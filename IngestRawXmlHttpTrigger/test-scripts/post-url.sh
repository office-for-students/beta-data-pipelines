# Run this script to POST a URL to the IngestRawXmlHtmlTrigger
# Azure Function. Modify as required to meet your specific 
# requirements.
curl --header "Content-Type: application/json" \
    --request POST \
    --data '{"resource_url":"http://somehost.com/somepath"}' \
    http://localhost:7071/api/IngestRawXmlHttpTrigger