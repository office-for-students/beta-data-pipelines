
def get_url_from_req(req):
    """Returns the value of resource_url from the HTTP request"""

    # TODO Update when we have finalised details on POST format
    try:
        req_json = req.get_json()
        url = req_json['resource_url']
    except (AttributeError, TypeError, ValueError, KeyError):
        raise OfsMissingUrlError

    return url


class OfsMissingUrlError(Exception):
   """Raised when the POST body does not contain the expected resource URL value"""
   pass
