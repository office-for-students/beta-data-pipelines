import logging


def get_url_from_req(req):
    """Returns the value of resource_url from the HTTP request"""
    try:
        req_body = req.get_json()
        url = req_body['resource_url']
    except (ValueError, KeyError):
        raise OfsMissingUrlError

    return url


class OfsMissingUrlError(Exception):
   """Raised when the POST body does not contain the expected resource URL value"""
   pass
