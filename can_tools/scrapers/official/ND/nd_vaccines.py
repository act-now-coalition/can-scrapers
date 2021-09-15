from typing import Any

import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard


class NDVaccineCounty(MicrosoftBIDashboard):

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("North Dakota").fips)

    source = "https://www.health.nd.gov/covid19vaccine/dashboard"
    source_name = "North Dakota Department of Health"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

    variables = {
        "at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "fully_vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def construct_body(self, ds_id, model_id, report_id):
        "Build body request"
        body = {}

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
                                            ("p", "Public Metrics", 0),
                                            ("c", "County", 0),
                                            ("s", "slicer_DoseSelector", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ("c", "County", "location_name")
                                        ],
                                        [],
                                        [
                                            # Measures
                                            (
                                                "p",
                                                "Public 1 dose coverage rate numerator 12 up",
                                                "total_vaccine_initiated",
                                            ),
                                            (
                                                "p",
                                                "Public Up-to-Date coverage rate numerator 12 up",
                                                "total_vaccine_completed",
                                            ),
                                        ],
                                    ),
                                }
                            }
                        }
                    ],
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
        body = self.construct_body(ds_id, model_id, report_id)

        res = self.sess.post(url, json=body, headers=headers)

        return res.json()

    def normalize(self, resjson: Any) -> pd.DataFrame:
        foo = resjson["results"][0]["result"]["data"]
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]

        # Build dict of dicts with relevant info
        col_mapping = {
            "C_0": "county",
            "C_1": "at_least_one_dose",
            "C_2": "fully_vaccinated",
        }
        data_rows = []

        # Iterate through all of the rows and store relevant data
        # skip first entry, which does not contain any data
        for record in data[1:]:
            flat_record = flatten_dict(record)
            row = {}
            for k in list(col_mapping.keys()):
                flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                if len(flat_record_key) > 0:
                    row[col_mapping[k]] = flat_record[flat_record_key[0]]

            data_rows.append(row)

        data = self._rename_or_add_date_and_location(
            data=pd.DataFrame(data_rows),
            location_name_column="county",
            location_names_to_drop=["ND-COUNTY UNKNOWN"],
            location_names_to_replace={
                "Lamoure": "LaMoure",
                "Mchenry": "McHenry",
                "Mcintosh": "McIntosh",
                "Mckenzie": "McKenzie",
                "Mclean": "McLean",
            },
            timezone="US/Central",
        ).pipe(self._reshape_variables, variable_map=self.variables)
        return data
