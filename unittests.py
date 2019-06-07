import json
import unittest
import azure.functions as func
from IngestRawXmlHttpTrigger.utils import get_url_from_req, OfsMissingUrlError


class TestGetUrlFromReq(unittest.TestCase):
    def setUp(self):
        self.test_url = 'http://somehost.com/somepath'
        self.valid_data = {
            'resource_url': self.test_url}
        self.invalid_data = {
            'invalid_name': self.test_url}

    def test_with_valid_data(self):
        """Test with valid body data"""
        valid_body = json.dumps(self.valid_data)
        http_req = func.HttpRequest(
            method='post', url='http://somehost.com/somepath', body=valid_body.encode('ASCII'))
        url = get_url_from_req(http_req)
        self.assertEqual(url, self.test_url)

    def test_with_invalid_data(self):
        """Test with invalid POST body data"""
        invalid_body = json.dumps(self.invalid_data)
        http_req = func.HttpRequest(
            method='post', url='http://somehost.com/somepath', body=invalid_body.encode('ASCII'))
        with self.assertRaises(OfsMissingUrlError):
            get_url_from_req(http_req)


if __name__ == '__main__':
    unittest.main()
