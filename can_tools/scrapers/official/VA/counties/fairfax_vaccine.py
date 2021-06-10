import json

from typing import Any

import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard


class VirginiaFairfaxVaccine(MicrosoftBIDashboard):

    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Virginia").fips)

    source = "https://www.fairfaxcounty.gov/health/novel-coronavirus/vaccine/data"
    source_name = "Fairfax County, Virginia"
    powerbi_url = "https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net"

    variables = {
        "vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "vaccine_completed": variables.FULLY_VACCINATED_ALL,
    }

    complete_where = [
        {
            "Condition": {
                "In": {
                    "Expressions": [
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "c"}},
                                "Property": "Vaccinated Status",
                            }
                        }
                    ],
                    "Values": [[{"Literal": {"Value": "'Fully'"}}]],
                }
            }
        },
    ]

    init_where = [
        {
            "Condition": {
                "In": {
                    "Expressions": [
                        {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "c"}},
                                "Property": "Derived Dose Number",
                            }
                        }
                    ],
                    "Values": [[{"Literal": {"Value": "1L"}}]],
                }
            }
        }
    ]

    def construct_body(
        self, resource_key, ds_id, model_id, report_id, where_clause, dose_name
    ):
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
                                            (
                                                "c",
                                                "COVID19_Vax_Admn_KPI_Fairfax_Master",
                                                0,
                                            )
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [],
                                        [],
                                        [
                                            (
                                                "c",
                                                "Received One Dose",
                                                dose_name,
                                            )
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

        # make 2 calls, one for init one for complete
        body = self.construct_body(
            resource_key,
            ds_id,
            model_id,
            report_id,
            self.init_where,
            "vaccine_initiated",
        )
        init_res = self.sess.post(url, json=body, headers=headers).json()
        body = self.construct_body(
            resource_key,
            ds_id,
            model_id,
            report_id,
            self.complete_where,
            "vaccine_completed",
        )
        complete_res = self.sess.post(url, json=body, headers=headers).json()

        return init_res, complete_res

    def normalize(self, resjson):

        dfs = []
        for dose in resjson:
            foo = dose["results"][0]["result"]["data"]
            descriptor = foo["descriptor"]["Select"]
            data = foo["dsr"]["DS"][0]["PH"][0]["DM0"]

            # Build dict of dicts with relevant info
            col_mapping = {x["Value"]: x["Name"] for x in descriptor}
            col_keys = list(col_mapping.keys())

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
            dfs.append(df.melt())

        return (
            pd.concat(dfs)
            .assign(location=51059, vintage=self._retrieve_vintage())
            .pipe(
                self._rename_or_add_date_and_location,
                location_column="location",
                timezone="US/Eastern",
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(columns={"variable"})
        )
