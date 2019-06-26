import json
import unittest
import azure.functions as func
from IngestRawXmlHttpTrigger.utils import get_url_from_req, OfsMissingUrlError


class TestGetUrlFromReq(unittest.TestCase):
    def setUp(self):
        self.test_url = 'http://thehostwegetxmlfrom.com/somepath'
        self.valid_json = {
            'resource_url': self.test_url}
        self.invalid_json_name = {
            'invalid_json_name': self.test_url}
        self.empty_json_object = "{}"
        self.empty_body = ""

    def test_with_valid_json(self):
        """Test with valid json"""
        valid_body = json.dumps(self.valid_json)
        http_req = func.HttpRequest(
            method='post', url='http://somehost.com/somepath', body=valid_body.encode('ASCII'))
        url = get_url_from_req(http_req)
        self.assertEqual(url, self.test_url)

    def test_with_invalid_json_name(self):
        """Test with invalid json name"""
        invalid_body = json.dumps(self.invalid_json_name)
        http_req = func.HttpRequest(
            method='post', url='http://somehost.com/somepath', body=invalid_body.encode('ASCII'))
        with self.assertRaises(OfsMissingUrlError):
            get_url_from_req(http_req)

    def test_with_empty_json_object(self):
        """Test with empty json object"""
        invalid_body = json.dumps(self.empty_json_object)
        http_req = func.HttpRequest(
            method='post', url='http://somehost.com/somepath', body=invalid_body.encode('ASCII'))
        with self.assertRaises(OfsMissingUrlError):
            get_url_from_req(http_req)

    def test_with_empty_body(self):
        """Test with empty POST body"""
        http_req = func.HttpRequest(
            method='post', url='http://somehost.com/somepath', body=self.empty_body.encode('ASCII'))
        with self.assertRaises(OfsMissingUrlError):
            get_url_from_req(http_req)


if __name__ == '__main__':
    unittest.main()
