import json
import os
from typing import List, Union

import pandas as pd
import requests

STATE_FIPS = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
]


def _download_data_file():
    url = "https://api.census.gov/data.json"
    available_data = requests.get(url)

    return available_data.json()


def _update_data_file(filename: str):
    "Saves the available datasets into a file called `filename`"
    data = _download_data_file()

    with open(filename, "w") as f:
        json.dump(data, f)


def _load_metadata(filename: str):
    "Loads the available datasets from a file called `filename`"
    if not os.path.isfile(filename):
        _update_data_file(filename)

    with open(filename, "r") as f:
        available_data = json.load(f)

    return available_data["dataset"]


def determine_valid_geographies(geo_url: str):
    # TODO: This is a method so that in the future, we can pull the
    #       list of valid geographies from `dataset["c_geographyLink"]`
    #       rather than impose this small subset of geographies
    VALID_GEOGRAPHIES = {
        "us": "us:*",
        # "region": "region:*",
        # "division": "division:*",
        "state": "state:*",
        "county": "county:*",
        "tract": "tract:*"
        # "msa": "metropolitan%20statistical%20area/micropolitan%20statistical%20area:*",
        # "csa": "combined%20statistical%20area:*",
        # "puma": "public%20use%20microdata%20area:*"
    }

    return VALID_GEOGRAPHIES


def process_geography_args(geography, geo_url: str = ""):
    """
    Takes the geography arguments and turns them into a string
    that can be an input to a request

    Parameters
    ----------
    geography : dict, str, or tuple
        The relevant geography for the data. If `geography` is a
        string, it must be one of the values in `VALID_GEOGRAPHIES`
        and you will receive values for all of the corresponding
        geographies. If `geography` is a tuple then the first
        element should be a geography in `VALID_GEOGRAPHIES` and
        the second element should be a collection of values that
        are subsets of that geography. If `geography` is a dict, it
        should use have the keys `for` and `in` as described in the
        US Census documentation --- The values of these keys should
        be tuples with a geography as the first element and a
        collection of values as the second element as described
        above.
    """
    valid_geographies = determine_valid_geographies(geo_url)
    out = ""

    if isinstance(geography, str):
        is_valid = geography in valid_geographies.keys()
        if not is_valid:
            msg = "If you pass a string, you must use one of the "
            msg += "geographies in the `VALID_GEOGRAPHIES` dict."
            raise ValueError(msg)

        out = "&for=" + valid_geographies[geography]

    elif isinstance(geography, dict):
        is_valid = "for" in geography.keys()
        if not is_valid:
            msg = "If you pass a dict, you must have keys 'for' and 'in'"
            raise ValueError(msg)

        out = "&for=" + geography["for"][0]
        out += ":" + ",".join(map(str, geography["for"][1]))

        if "in" in geography.keys():
            out += "&in=" + geography["in"][0]
            out += ":" + ",".join(map(str, geography["in"][1]))

    elif isinstance(geography, tuple):
        is_valid = geography[0] in valid_geographies.keys()

        out = "&for=" + geography[0]
        out += ":" + ",".join(map(str, geography[1]))

    return out


def process_column_args(columns: List[str]):
    """
    Takes the column arguments and turns them into a string that
    can be an input to a request

    Parameters
    ----------
    columns : list(str)
        The variables that should be retrieved from Census
    """
    # TODO: Validate the variable arguments
    if isinstance(columns, str):
        columns = [columns]

    return "&get=" + ",".join(columns)


def _process_get_json(columns: list, req_json: dict):
    """
    Converts a particular request into a DataFrame

    Parameters
    ----------
    columns : list
        A list of the numeric columns (variables) of the table
    req_json : dict
        The json of the request from `self._get`

    Returns
    -------
    df : pd.DataFrame
        A DataFrame representation of the data in req_json
    """
    df = pd.DataFrame(data=req_json[1:], columns=req_json[0])
    df[columns] = df[columns].apply(lambda x: pd.to_numeric(x, errors="ignore"))

    return df


class USCensusBaseAPI:
    """
    Base class for accessing the US Census API

    Parameters
    ----------
    dataset : dict
        One of the JSON dict objects from
        https://api.census.gov/data.json
    key : str
        API key
    """

    def __init__(self, dataset: dict, key: str):
        self.api_url = dataset["distribution"][0]["accessURL"]
        self.dataset = dataset
        self.key = key

    def _get(self, columns: List[str], geography: Union[dict, tuple, str]):
        """
        This is the hidden get method that actually fetches the data
        from the US Census API.

        Parameters
        ----------
        columns : list(string)
            The data columns that you would like
        geography : str, dict, or tuple
            See documentation for `self.process_geography_args`
        """
        # Build up the request url
        req_url = self.api_url + f"?key={self.key}"
        req_url += process_column_args(columns)
        req_url += process_geography_args(geography)

        req = requests.get(req_url)

        if req.status_code != 200:
            print(req.status_code)
            print(req.text)
            msg = "API request failed... See printout above for more info"
            raise ValueError(msg)

        return req

    def get(self, columns: List[str], geography: Union[list, str, dict]):
        """
        Retrieves the data from a particular US Census data source

        Parameters
        ----------
        columns : list(string)
            The data columns that you would like
        geography : str, dict, or tuple
            See documentation for `self.process_geography_args`
        """
        # Perform actual request... If it is a tract then we have to
        # do the requests state by state...
        if geography == "tract":
            dfs = []
            for state_fips in STATE_FIPS:
                req = self._get(
                    columns, {"for": ("tract", "*"), "in": ("state", [state_fips])}
                )
                req_json = req.json()
                _df = _process_get_json(columns, req_json)
                dfs.append(_df)
            df = pd.concat(dfs, axis="index")
        else:
            req = self._get(columns, geography)
            req_json = req.json()
            df = _process_get_json(columns, req_json)

        return df


class ACSAPI(USCensusBaseAPI):
    """
    The American Community Survey (ACS) is an ongoing survey that
    provides data every year -- giving communities the current
    information they need to plan investments and services. The ACS
    covers a broad range of topics about social, economic, demographic,
    and housing characteristics of the U.S. population. Much of the ACS
    data provided on the Census Bureau's Web site are available
    separately by age group, race, Hispanic origin, and sex.

    The 1 year files incorporates a single year's worth of data
    collection and provides detailed information for areas with a
    population of more than 65,000.

    The 5 year files incorporates five year's worth of data into it's
    estimates and provides detailed information for all census areas
    down to the census block level.

    For more information, please refer to:
        * https://www.census.gov/data/developers/data-sets/acs-1year.html
        * https://www.census.gov/data/developers/data-sets/acs-5year.html

    Parameters
    ----------
    product : string
        Which ACS product to use. Can either be `acs1` or `acs5`
    table : string
        Which ACS table that will be queried for data
    year : int
        Which year of data will be queried
    key : string
        The US Census API key.
    """

    def __init__(self, product: str, table: str, year: int, key: str):
        # Store table and vintage
        self.product = "acs1" if "1" in str(product) else "acs5"
        self.table = table
        self.year = year

        # Search for the dataset with the right properties
        _avail_datasets = _download_data_file()["dataset"]
        dataset = [
            x
            for x in _avail_datasets
            if (
                (self.product in x["c_dataset"])
                and (table.title() in x["title"])
                and (x["c_vintage"] == year)
            )
        ]
        if len(dataset) > 1:
            raise ValueError("Data set cannot be determined with table/year info")

        super(ACSAPI, self).__init__(dataset=dataset[0], key=key)
