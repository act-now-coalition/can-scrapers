import json

from typing import Any

import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard


class {SCRAPER NAME}(MicrosoftBIDashboard):
    """
    Fetch county level vaccine data from a Microsoft PowerBI dashboard

    Examples include:
      * can_tools.scrapers.official.MN.mn_vaccines.py
      * can_tools.scrapers.official.NV.nv_vaccines.py
      * can_tools.scrapers.official.PA.pa_vaccines.py
    """

    has_location = {TRUE IF HAS FIPS CODE FALSE ELSE}
    location_type = {LOCATION TYPE}
    state_fips = int(us.states.lookup({STATE NAME}).fips)

    source = {SOURCE URL}
    powerbi_url = {POWER BI URL}

    def construct_body(self, resource_key, ds_id, model_id, report_id):
        "Build body request"
        body = {}

        # Set version
        # TODO: You should verify that this is the same in your scraper -- It
        # has been for most of the scrapers we've written so we included it
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
                                            # From
                                            ({CAN SOURCE REF}, {BI SOURCE REF}, {SOURCE TYPE})
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ({CAN SOURCE REF}, {SOURCE PROPERTY}, {CAN NAME})
                                        ],
                                        [
                                            # Aggregations
                                            ({CAN SOURCE REF}, {SOURCE PROPERTY}, {AGG FUNC}, {CAN NAME})
                                        ],
                                        [
                                            # Measures
                                            ({CAN SOURCE REF}, {SOURCE PROPERTY}, {CAN NAME})
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

    def normalize(self, resjson):
        # Extract components we care about from json
        # TODO: The order of these outputs may differ slightly in
        #       different dashboards
        foo = resjson["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][0]["DM0"]

        # Build dict of dicts with relevant info
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}
        col_keys = list(col_mapping.keys())

        # TODO: Figure out how to iterate through all of the rows and
        #       store relevant data
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

        # TODO: Manipulate data to desired shape

        # Reshape
        crename = {}
        out = df.melt(id_vars=["location_name"])

        # Add CMU, dt, vintage
        out = self.extract_CMU(out, crename)

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
