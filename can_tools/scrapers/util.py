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
    kwargs = dict(backoff_factor=backoff_factor)
    kwargs.update(kw)
    adapter = HTTPAdapter(max_retries=Retry(**kwargs))
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    http.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0"
        }
    )

    return http


def flatten_dict(d, sep="_"):
    """
    Flattens a dictionary

    Credit to:

    https://medium.com/better-programming/how-to-flatten-a-dictionary-with-nested-lists-and-dictionaries-in-python-524fd236365
    """
    out = {}

    def recurse(t, parent_key=""):
        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            out[parent_key] = t

    recurse(d)

    return out
