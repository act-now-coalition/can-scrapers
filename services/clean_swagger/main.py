import copy
import logging
import os
import requests
from flask import Flask, jsonify
from flask_cors import CORS

# Change the format of messages logged to Stackdriver
logging.basicConfig(format="%(message)s", level=logging.INFO)

app = Flask(__name__)
CORS(app)

DESCRIPTION = """
# Rest API

The REST API does not require any software to be installed on your computer, it allows you to
interact with our data through standard web protocols.

Our API is build using [PostgREST](http://postgrest.org/en/v7.0.0/). For more information on the
different features of this API, please see their [documentation](http://postgrest.org/en/v7.0.0/api.html).

## API Key

Our data is free and open for anyone to use (and always will be). Our team agreed that this was central to our mission when we agreed to begin this project. However, we do find it useful to have information about our users and to see how they use the data for two reasons:

1. It helps us focus and improve the datasets that are seeing the most use.
2. The number of users, as measured by active API keys, is one metric that we use to show that the
project is useful when we are discussing additional grant funding.

We are grateful to everyone who is willing to register for and use their API key when interacting
with our data.

To register for an API key, see the registration form [on our home page](https://covidcountydata.org#register).

After obtaining an API key, please attach it as either the `apikey` header or query parameter in future requests

For example, if my API key were `abc123` and I was getting data from `https://api.covidcountydata.org/covid_us?fips=eq.6`, I
would adjust the url to be `https://api.covidcountydata.org/covid_us?fips=eq.6&apikey=abc123`


## Request structure

In order to be able to request data, you will need to know which endpoint the data comes from and,
optionally, which filters you would like to apply to the data before it's delivered:


**API Endpoints**

The data in our database is broken into tables of related information with a separate endpoint for
each table. For example, all of the U.S. COVID-19 related data is stored in the `covid_us` table
and it has a corresponding endpoint of `https://api.covid.valorum.ai/covid_us`. For a complete
list of the available endpoints, see the interactive playground near the bottom of the page.


**Filters**

There are a two types of parameters that can be included as filters:

* _Data parameters_: Data parameters are used to select subsets of the data by performing some
  type of comparison between the parameter argument and the data.
* _Keyword parameters_: Keyword parameters interact with how the data is returned. For example,
  `select` can modify which columns are returned, `order` changes how the data is ordered when it
  is returned, and `limit` changes the number of observations returned.

The data parameters that you are able to apply will depend on what columns are available in a
particular dataset. Many of the tables will have the columns `dt` (date of the obervation),
`location` (a geographical identifier, for the U.S. this is often the fips code), `variable` (name
of the variable being observed), and `value` (the value of the variable in that location at that
time).

For example, if you wanted to request 5 observations of the `tests_total` variable from after
August 1, 2020 then you would use the following query:

`https://api.covid.valorum.ai/covid_us?dt=gt.2020-08-01&variable=eq.tests_total&limit=5`

Of course, rather than input this address into your browser, you could query the API for this
information using a more generic tool such as `curl` or `javascript`.

We make two additional observations:

1. Rather than use `>`, `<`, `=`, etc..., the REST API expects you to use `gt.`, `lt.`, `eq.`,
   etc... For a complete list of comparisons, see [PostgREST documentation](http://postgrest.org/en/v7.0.0/api.html#operators)
2. Much of the data is stored in _long form_ rather than _wide form_. In long form each data
  observation, or value, is associated with its unique identifiers. For example, in many of our
  datasets the identifiers are `dt`, `location`, and `variable`, and the value of the observation
  is stored in `value`. This can be seen in the example section below.


### Examples

We will do some examples to illustrate these points using the example table below. We will demonstrate
the example api requests using the `covid_us` endpoint (at https://api.covidcountydata.org/covid_us),
but the same concepts apply to all endpoints. Note that we will add `limit=20` to most of the requests
below to prevent unnecessary data transfer, but removing this query parameter will allow you to fetch the
whole dataset. Finally, note also that we'll use the same `apikey=abc123` argument as shown in the
example above, but the `abc123` should be replaced by your API key.

Below is an example table showing the structure of the data returned by the `covid_us` endpoint:


| dt         | fips | variable             | value  |
| ---------- | ---- | -------------------- | ------ |
| 2020-06-01 | 6    | deaths_total         | 4251   |
| 2020-06-01 | 12   | deaths_total         | 2543   |
| 2020-06-01 | 48   | deaths_total         | 1678   |
| 2020-06-02 | 6    | deaths_total         | 4286   |
| 2020-06-02 | 12   | deaths_total         | 2613   |
| 2020-06-02 | 48   | deaths_total         | 1698   |
| 2020-06-01 | 6    | positive_tests_total | 113006 |
| 2020-06-01 | 12   | positive_tests_total | 56830  |
| 2020-06-01 | 48   | positive_tests_total | 64880  |
| 2020-06-02 | 6    | positive_tests_total | 115310 |
| 2020-06-02 | 12   | positive_tests_total | 57447  |
| 2020-06-02 | 48   | positive_tests_total | 66568  |


### Only data from CA

In order to select data from only California (fips code 06) we filter the parameter `location` to
only be 6

`https://api.covid.valorum.ai/covid_us?fips=eq.06&limit=20&apikey=abc123`


### Only observations with more than 100,000 tests

In order to only select observations with more than 100,000 tests, we would want to use the following parameters

`https://api.covid.valorum.ai/covid_us?variable=eq.positive_tests_total&value=gt.100000&limit=20&apikey=abc123`


### Only observations after June 1, 2020

In order to only select the data from after June 1, 2020, we would use the following parameter

`https://api.covid.valorum.ai/covid_us?dt=gt.2020-06-01&limit=20&apikey=abc123`


### Select total deaths for Texas ordered by date

In order to select only the total deaths variable for Texas and have the results be ordered by date, we would use the following parameters

`https://api.covid.valorum.ai/covid_us?location=eq.48&variable=eq.positive_tests_total&order=dt&limit=20&apikey=abc123`


## Software

We use the following open source technologies to build this API.

* The data is hosted in a [PostgreSQL database](https://www.postgresql.org/)
* The API is built using [PostgREST](http://postgrest.org/en/v7.0.0/)
* The documentation is generated using [RapiDoc](https://mrin9.github.io/RapiDoc/).

We are grateful to all of these projects for simplifying the task of building, deploying, and
documenting our API. We are also grateful to Google Cloud for helping us host and distribute our
data.


## API Endpoints

"""
dataset_names = {
    "covid_us": "County-level covid data",
    "covid_historical": "Vintage county-level covid data",
    "covid_sources": "Source information for US COVID datasets",
    "economics": "Economic data",
    "demographics": "Demographics",
    "economic_snapshots": "Industry-specific GDP",
    "mobility_devices": "Mobility devices",
    "mobility_locations": "Mobility locations",
    "hhs": "HHS data",
    "us_counties": "US counties",
    "us_states": "US states",
    "npi_us": "Nonpharmaceutical interventions data",
    "covid_global": "Our World in Data covid data",
    "covidtrackingproject": "Covid Tracking Project data",
    "nytimes_covid": "New York Times covid data",
    "usafacts_covid": "USA Facts covid data",
    "covid_sources": "Sources",
}

API_URL = os.environ.get("API_URL", "https://api.covidcountydata.org")
if API_URL is None:
    raise ValueError("API_URL not found for the ccd api")


@app.route("/")
def clean_swagger():
    res = requests.get(
        "https://api.covid.valorum.ai",
        headers=dict(apikey="R9EUXZbvHEMxfowsNtEtNzRw6d4HtrYf"),
    )
    if not res.ok:
        raise ValueError("Could not request original swagger")

    js = copy.deepcopy(res.json())
    methods = ["post", "patch", "put", "delete"]
    for path, routes in js["paths"].items():
        for method in methods:
            if method in routes:
                del routes[method]

        for surviving_method, details in routes.items():
            oid = "{}{}".format(surviving_method, path.replace("/", "_"))
            details["operationId"] = oid
            details["security"] = [{"APIKeyHeader": []}]

    js["paths"]["/swagger.json"] = copy.deepcopy(js["paths"]["/"])
    del js["paths"]["/"]

    js["host"] = "api.covidcountydata.org"
    js["info"]["title"] = ""
    js["info"]["description"] = DESCRIPTION
    js["info"]["version"] = ""
    js["schemes"] = ["https"]
    js["securityDefinitions"] = {
        "APIKeyHeader": {
            "type": "apiKey", "in": "header", "name": "apikey"
        },
    }
    # js["security"] = [
    #     "APIKeyHeader",
    # ]

    # Set limit default to 100
    js["parameters"]["limit"]["default"] = 100

    for key in js["definitions"]:
        js["definitions"][key]["name"] = dataset_names.get(
            key, key.replace("_", " ").title()
        )

    return jsonify(js)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
