from typing import Any

import pandas as pd
import us
import os

from can_tools.scrapers import CMU, variables
from can_tools.scrapers.official.base import MicrosoftBIDashboard
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers import variables as v


class MinnesotaCountyVaccines(MicrosoftBIDashboard):
    """
    Fetch county level vaccine data from Pennsylvania's PowerBI dashboard
    """

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Minnesota").fips)

    source = "https://mn.gov/covid19/vaccine/data/index.jsp"
    source_name = "Minnesota Covid-19 Response"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

    # get the iframe link manually to bypass captcha
    # this will need to be updated periodically -- find the iframe in the page source html

    powerbi_dashboard_link = "https://app.powerbigov.us/view?r=eyJrIjoiMTE5ZDk0N2YtYzFlZC00ODg4LTkzMDYtMjM0ZWEwNjdmMTg3IiwidCI6ImViMTRiMDQ2LTI0YzQtNDUxOS04ZjI2LWI4OWMyMTU5ODI4YyJ9"

    def get_dashboard_iframe(self):
        fumn = {"src": self.powerbi_dashboard_link}

        return fumn

    def construct_body(self, resource_key, ds_id, model_id, report_id):
        body = {}

        # Set version
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        body["queries"] = [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": self.construct_from(
                                        [
                                            (
                                                "m",
                                                "MODEL_Joined_Counties",
                                                0,
                                            ),
                                            ("_", "_Measures_Used", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            (
                                                "m",
                                                "Client_County",
                                                "county",
                                            ),
                                        ],
                                        [],
                                        [
                                            (
                                                "_",
                                                "_displayCaseIncomplete",
                                                "total_vaccine_initiated_display",
                                            ),
                                            (
                                                "_",
                                                "_displayCaseComplete",
                                                "total_vaccine_completed_display",
                                            ),
                                        ],
                                    ),
                                }
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": self.construct_application_context(
                    ds_id, report_id
                ),
            }
        ]

        return body

    def fetch(self):

        # Get general information
        self._setup_sess()
        dashboard_frame = self.get_dashboard_iframe()
        resource_key = self.get_resource_key(dashboard_frame)

        ds_id, model_id, report_id = self.get_model_data(resource_key)
        # Get the post url
        url = self.powerbi_query_url()

        # Build post headers
        headers = self.construct_headers(resource_key)

        # Build post body
        body = self.construct_body(resource_key, ds_id, model_id, report_id)

        res = self.sess.post(url, json=body, headers=headers)

        return res.json()

    def normalize(self, resjson: dict) -> pd.DataFrame:
        # Extract components we care about from json
        foo = resjson["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]

        # Build dict of dicts with relevant info
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}
        col_keys = list(col_mapping.keys())

        # Iterate through all of the rows and store relevant data
        data_rows = []
        row_names = [col_mapping[desc["N"]] for desc in data[0]["S"]]
        for record in data:
            data_rows.append(record["C"])

        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows, columns=row_names)

        # Title case and remove the word county
        df["location_name"] = (
            df["county"].str.title().str.replace("County", "").str.strip()
        )
        df = df.query("location_name not in ['Unknown/Missing', 'Total 18+ Cdc']")

        # Rename certain counties
        df = df.replace(
            {
                "location_name": {
                    "Lac Qui Parle": "Lac qui Parle",
                    "Mcleod": "McLeod",
                    "Lake Of The Woods": "Lake of the Woods",
                }
            }
        )

        # Turn strings into numbers
        df["total_vaccine_initiated"] = pd.to_numeric(
            df["total_vaccine_initiated_display"].str.replace("L", "")
        )
        df["total_vaccine_completed"] = pd.to_numeric(
            df["total_vaccine_completed_display"].str.replace("L", "")
        )

        # Reshape
        crename = {
            "total_vaccine_initiated": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "total_vaccine_completed": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }
        out = df.melt(id_vars=["location_name"], value_vars=crename.keys())

        # Add CMU, dt, vintage
        out = self.extract_CMU(out, crename)
        out["dt"] = self._retrieve_dt("US/Central")
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep].dropna()


class MinnesotaCountyAgeVaccines(MinnesotaCountyVaccines):

    demographic = "age"
    col_mapping = {
        "G0": "location_name",
        "M_1_DM3_{demo}_C_1": "initiating",
        "M_1_DM3_{demo}_C_2": "completing",
    }
    # use these keys to fill in the keys for each dose type
    demographic_key = {
        "0": "12-15",
        "1": "16-17",
        "2": "18-49",
        "3": "50-64",
        "4": "65_plus",
    }
    variables = {
        "initiating": v.INITIATING_VACCINATIONS_ALL,
        "completing": v.FULLY_VACCINATED_ALL,
    }

    def construct_body(self, resource_key, ds_id, model_id, report_id, county):

        "Build body request"
        body = {}

        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        where_clause = [
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "m"}},
                                    "Property": "County",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{county}'"}}]],
                    }
                },
            },
        ]

        body["queries"] = [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": self.construct_from(
                                        [
                                            # From
                                            ("d", "dashboarddata", 0),
                                            ("_", "_Measures_Used", 0),
                                            ("a", "Age group POP", 0),
                                            ("m", "MODEL_Joined_Counties", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ("m", "County", "county"),
                                            (
                                                "d",
                                                "age",
                                                "demo",
                                            ),
                                        ],
                                        [
                                            # Aggregations
                                        ],
                                        [
                                            # Measures
                                            ("_", "casesincomplete", "incomplete"),
                                            ("_", "CasesCompleted", "complete"),
                                        ],
                                    ),
                                    "Where": where_clause,
                                }
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": self.construct_application_context(
                    ds_id, report_id
                ),
            }
        ]
        return body

    def fetch(self):
        # Get general information
        self._setup_sess()
        dashboard_frame = self.get_dashboard_iframe()
        resource_key = self.get_resource_key(dashboard_frame)
        ds_id, model_id, report_id = self.get_model_data(resource_key)

        # Get the post url
        url = self.powerbi_query_url()

        # Build post headers
        headers = self.construct_headers(resource_key)
        counties = self._retrieve_counties()

        jsons = []
        """
        make one call per county to ensure that all the data is received
        """
        for county in counties:
            print("making request for: ", county)
            body = self.construct_body(resource_key, ds_id, model_id, report_id, county)
            res = self.sess.post(url, json=body, headers=headers)
            jsons.append(res.json())
        return jsons

    def normalize(self, resjson: dict) -> pd.DataFrame:
        data = []
        for chunk in resjson:
            foo = chunk["results"][0]["result"]["data"]
            d = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
            data.extend(d)

        # Iterate through all of the rows and store relevant data
        data_rows = []
        for record in data:
            flat_record = flatten_dict(record)
            # print(flat_record)

            for demo_key, demo in self.demographic_key.items():
                row = {}
                row[self.demographic] = demo
                for k in list(self.col_mapping.keys()):
                    k_formatted = k.format(demo=demo_key)
                    flat_record_key = [
                        frk for frk in flat_record.keys() if k_formatted in frk
                    ]
                    if len(flat_record_key) > 0:
                        row[self.col_mapping[k]] = flat_record[flat_record_key[0]]
                data_rows.append(row)

        # combine into dataframe
        df = pd.DataFrame.from_records(data_rows)
        out = self._reshape_variables(
            df,
            self.variables,
            id_vars=[self.demographic],
            skip_columns=[self.demographic],
        )

        return out.assign(
            location_name=lambda x: x["location_name"].str.title(),
            dt=self._retrieve_dtm1d("US/Eastern"),
            vintage=self._retrieve_vintage(),
        ).replace(
            {
                "Mcleod": "McLeod",
                "Lac Qui Parle": "Lac qui Parle",
                "Lake Of The Woods": "Lake of the Woods",
            }
        )


class MinnesotaCountySexVaccines(MinnesotaCountyAgeVaccines):
    demographic = "sex"
    # use these keys to fill in the keys for each dose type
    demographic_key = {"0": "female", "1": "male", "2": "unknown"}

    def construct_body(self, resource_key, ds_id, model_id, report_id, county):

        "Build body request"
        body = {}

        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        where_clause = [
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "m"}},
                                    "Property": "County",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{county}'"}}]],
                    }
                }
            },
            {
                "Condition": {
                    "Not": {
                        "Expression": {
                            "In": {
                                "Expressions": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "d"}
                                            },
                                            "Property": "gender",
                                        }
                                    }
                                ],
                                "Values": [[{"Literal": {"Value": "'Other'"}}]],
                            }
                        }
                    }
                }
            },
        ]

        body["queries"] = [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": self.construct_from(
                                        [
                                            # From
                                            ("d", "dashboarddata", 0),
                                            ("_", "_Measures_Used", 0),
                                            ("a", "Age group POP", 0),
                                            ("m", "MODEL_Joined_Counties", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ("m", "County", "county"),
                                            ("d", "gender", "demo"),
                                        ],
                                        [
                                            # Aggregations
                                        ],
                                        [
                                            # Measures
                                            ("_", "casesincomplete", "incomplete"),
                                            ("_", "CasesCompleted", "complete"),
                                        ],
                                    ),
                                    "Where": where_clause,
                                }
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": self.construct_application_context(
                    ds_id, report_id
                ),
            }
        ]
        return body
