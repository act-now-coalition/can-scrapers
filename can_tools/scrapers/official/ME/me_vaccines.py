from typing import Any

import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import MicrosoftBIDashboard
from can_tools.scrapers.util import flatten_dict


class MaineCountyVaccines(MicrosoftBIDashboard):
    """
    Fetch county level vaccine data from Maine's PowerBI dashboard
    """

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Maine").fips)

    source = "https://www.maine.gov/covid19/vaccines/dashboard"
    source_name = "Covid-19 Response Office of the Governor"
    powerbi_url = "https://wabi-us-east-a-primary-api.analysis.windows.net"

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
                                                "i",
                                                "Patient Geographic Attributes",
                                                0,
                                            ),
                                            (
                                                "c",
                                                "COVID Vaccination Summary Measures",
                                                0,
                                            ),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            (
                                                "i",
                                                "Geographic County Name",
                                                "county",
                                            ),
                                        ],
                                        [],
                                        [
                                            (
                                                "c",
                                                "Doses Administered",
                                                "total_vaccine_administered",
                                            ),
                                            (
                                                "c",
                                                "First Dose",
                                                "total_vaccine_initiated",
                                            ),
                                            (
                                                "c",
                                                "Final Dose",
                                                "total_vaccine_completed",
                                            ),
                                            (
                                                "c",
                                                "Population First Dose %",
                                                "total_vaccine_initiated_percent",
                                            ),
                                            (
                                                "c",
                                                "Population Final Dose %",
                                                "total_vaccine_completed_percent",
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
            Crecord = record["C"]
            if "County" not in str(Crecord[0]):
                continue
            data_rows.append(record["C"])

        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows, columns=row_names)

        # Title case and remove the word county
        df["location_name"] = df["county"].str.replace("County, ME", "").str.strip()

        # Change into percentage
        for col in [
            "total_vaccine_initiated_percent",
            "total_vaccine_completed_percent",
        ]:
            df.loc[:, col] = 100 * df.loc[:, col]

        # Reshape
        crename = {
            "total_vaccine_administered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
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
            "total_vaccine_initiated_percent": CMU(
                category="total_vaccine_initiated",
                measurement="current",
                unit="percentage",
            ),
            "total_vaccine_completed_percent": CMU(
                category="total_vaccine_completed",
                measurement="current",
                unit="percentage",
            ),
        }
        out = df.melt(id_vars=["location_name"], value_vars=crename.keys()).dropna()

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
