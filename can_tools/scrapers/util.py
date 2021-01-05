import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session(backoff_factor=0.1, **kw):
    """
    Create a requests Session that will retry http
    reqeusts on failure. All keyword arguments are passed
    directly to `requests.packages.urllib3.util.retry.Retry`

    The only kwarg we provide a default value for is
    `backoff_factor`
    """
    kwargs = dict(backoff_sfactor=backoff_factor)
    kwargs.update(kw)
    adapter = HTTPAdapter(max_retries=Retry(**kw))
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    return http
