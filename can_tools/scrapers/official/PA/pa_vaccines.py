import json

from typing import Any

import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard


class PennsylvaniaCountyVaccines(MicrosoftBIDashboard):
    """
    Fetch county level vaccine data from Pennsylvania's PowerBI dashboard
    """

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Pennsylvania").fips)

    source = "https://www.health.pa.gov/topics/disease/coronavirus/Vaccine/Pages/Vaccine.aspx"
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
                                            (
                                                "c",
                                                "Counts of People by County",
                                                0,
                                            )
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            ("c", "County", "location_name"),
                                            (
                                                "c",
                                                "PartiallyCovered",
                                                "total_vaccine_initiated",
                                            ),
                                            (
                                                "c",
                                                "FullyCovered",
                                                "total_vaccine_completed",
                                            ),
                                        ],
                                        [],
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

    def normalize(self, resjson):
        # Extract components we care about from json
        foo = resjson["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][0]["DM0"]

        # Build dict of dicts with relevant info
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}
        col_keys = list(col_mapping.keys())

        # Iterate through all of the rows and store relevant data
        data_rows = []
        for record in data:
            flat_record = flatten_dict(record)

            row = {}
            for k in col_keys:
                flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                if len(flat_record_key) > 0:
                    row[col_mapping[k]] = flat_record[flat_record_key[0]]

            data_rows.append(row)

        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows).dropna()
        df = df.query("location_name != '' & location_name != 'Out-of-State'")

        # Initiated is not at least one dose for PA
        df["total_vaccine_initiated"] = df.eval(
            "total_vaccine_initiated + total_vaccine_completed"
        )

        # Make sure McKean follows capitalization in db
        df = df.replace({"location_name": {"Mckean": "McKean"}})

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
        out = df.melt(id_vars=["location_name"])

        # Add CMU, dt, vintage
        out = self.extract_CMU(out, crename)
        out["dt"] = self._retrieve_dt("US/Eastern")
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
