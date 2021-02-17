from typing import Any

import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard


class NevadaCountyVaccines(MicrosoftBIDashboard):
    """
    Fetch county level vaccine data from Nevada's PowerBI dashboard

    TODO: They have time-series information in their Microsoft BI
          database but I can't quite figure out how to extract it
          in a way that makes the numbers line up... The following
          query components help extract the time-series data

    "Binding": {
        "Primary": {
            "Groupings": [
                {"Projections": [0]},
                {"Projections": [1]},
                {"Projections": [2]},
                {"Projections": [3]},
            ]
        },
        "DataReduction": {
            "DataVolume": 3,
            "Primary": {
                "Window": {"Count": 30_000}
            }
        },
        "Version": 1
    },
    "Query": {
        "Version": 2,
        "From": self.construct_from(
            [
                ("c", "Counties", 0),
                ("s", "Sheet1", 0),
            ]
        ),
        "Select": self.construct_select(
            [
                ("c", "County", "county"),
                ("s", "Date", "dt"),
                ("s", "Doses Administered", "doses_administered"),
                ("s", "Doses Initiated", "doses_initiated"),
                ("s", "Fully Immunized", "doses_completed"),
            ],
            [],
            [],
        ),
    """

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Nevada").fips)

    source = "https://nvhealthresponse.nv.gov/"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

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
                                            ("c", "Counties", 0),
                                            ("s", "Sheet1", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            ("c", "County", "county"),
                                        ],
                                        [
                                            (
                                                "s",
                                                "Doses Initiated",
                                                0,
                                                "doses_initiated",
                                            ),
                                            (
                                                "s",
                                                "Fully Immunized",
                                                0,
                                                "doses_completed",
                                            ),
                                            (
                                                "s",
                                                "Doses Administered",
                                                0,
                                                "doses_administered",
                                            ),
                                        ],
                                        [],
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
        col_names = [x["N"] for x in data[0]["S"]]
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}

        # Iterate through all of the rows and store relevant data
        data_rows = []
        for row in data:
            data_rows.append(row["C"])

        # Dump records into a DataFrame
        df = (
            pd.DataFrame.from_records(data_rows, columns=col_names)
            .rename(columns=col_mapping)
            .rename(columns={"county": "location_name"})
        )

        # Reshape
        crename = {
            "doses_initiated": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "doses_completed": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "doses_administered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }
        out = df.melt(id_vars=["location_name"], value_vars=crename.keys())

        # Add CMU, dt, vintage
        out = self.extract_CMU(out, crename)
        out["dt"] = self._retrieve_dt("US/Pacific")
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

        return out.loc[:, cols_to_keep]
