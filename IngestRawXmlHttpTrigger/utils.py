import logging


def get_url_from_req(req):
    """Returns the value of resource_url from the HTTP request"""
    try:
        req_body = req.get_json()
        url = req_body['resource_url']
    except (ValueError, KeyError):
        raise MissingUrlError

    return url


class MissingUrlError(Exception):
   """Raised when the POST body does not contain a JSON encoded URL value"""
   pass
