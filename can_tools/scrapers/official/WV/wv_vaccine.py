from typing import Any

import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers import variables
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard


class WVCountyVaccine(MicrosoftBIDashboard):

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("West Virginia").fips)

    source = "https://dhhr.wv.gov/COVID-19/Pages/default.aspx"
    source_name = "West Virginia Department of Health & Human Resources"
    powerbi_url = "https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net"

    variables = {
        "partial": variables.INITIATING_VACCINATIONS_ALL,
        "fully": variables.FULLY_VACCINATED_ALL,
    }

    def construct_body(self, resource_key, ds_id, model_id, report_id, where_variable):
        body = {}

        # Set version
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        from_variables = [
            (
                "c",
                "vamsCOVID_Vaccine",
                0,
            ),
            (
                "d1",
                "dimFollow-Up Vaccine",
                0,
            ),
            (
                "d",
                "dimCounty",
                0,
            ),
        ]

        select_variables = [
            [
                ("d", "County Name", "dimCounty.County Name"),
            ],
            [],
            [
                ("c", "Count", "COVID_Vaccine.Count"),
            ],
        ]

        # TODO(chris): Make where query construction more generic
        where_query = [
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
                                            "Property": "County Name",
                                        }
                                    }
                                ],
                                "Values": [[{"Literal": {"Value": "null"}}]],
                            }
                        }
                    }
                }
            },
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "d1"}},
                                    "Property": "Updated Name",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{where_variable}'"}}]],
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
                                    "From": self.construct_from(from_variables),
                                    "Select": self.construct_select(*select_variables),
                                    "Where": where_query,
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

        self._setup_sess()
        dashboard_frame = self.get_dashboard_iframe()
        resource_key = self.get_resource_key(dashboard_frame)
        ds_id, model_id, report_id = self.get_model_data(resource_key)

        url = self.powerbi_query_url()

        headers = self.construct_headers(resource_key)

        # Because of the way WV presents their data, making two separate queries
        # to pull out the partially vaccinated and fully vaccinated individuals.
        body = self.construct_body(
            resource_key, ds_id, model_id, report_id, "People Partially Vaccinated"
        )
        res = self.sess.post(url, json=body, headers=headers)
        partial_vaccination_results = res.json()

        body = self.construct_body(
            resource_key, ds_id, model_id, report_id, "People Fully Vaccinated"
        )
        res = self.sess.post(url, json=body, headers=headers)
        total_vaccination_results = res.json()

        return {
            "partial": partial_vaccination_results,
            "fully": total_vaccination_results,
        }

    def _extract_results(self, results, column_name):
        # Extract components we care about from json
        foo = results["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]

        # Build dict of dicts with relevant info
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}

        col_keys = list(col_mapping.keys())

        # Iterate through all of the rows and store relevant data
        data_rows = []
        row_names = [col_mapping[desc["N"]] for desc in data[0]["S"]]
        for record in data:
            if "C" not in record:
                continue

            data_rows.append(
                {"location_name": record["C"][0], column_name: record["C"][1]}
            )

        return pd.DataFrame(data_rows).set_index("location_name")

    def normalize(self, resjson: dict) -> pd.DataFrame:
        data = pd.concat(
            [
                self._extract_results(resjson["partial"], "partial"),
                self._extract_results(resjson["fully"], "fully"),
            ],
            axis="columns",
        ).reset_index()

        data = self._rename_or_add_date_and_location(
            data,
            location_name_column="location_name",
            timezone="US/Eastern",
            apply_title_case=False,
        )
        data = self._reshape_variables(data, self.variables)

        return data
