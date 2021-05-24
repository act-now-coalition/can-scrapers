from typing import Any

import pandas as pd
import us
import os

from can_tools.scrapers import CMU, variables
from can_tools.scrapers.official.base import MicrosoftBIDashboard
from can_tools.scrapers.util import flatten_dict


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
                "COVID_Vaccine",
                0,
            ),
            (
                "d",
                "dimCounty",
                0,
            ),
            ("n", "Navigation", 0),
        ]

        select_variables = [
            [
                ("d", "County Name", "dimCounty.County Name"),
            ],
            [],
            [
                ("c", "Total Dose", "COVID_Vaccine.Total Dose"),
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
                                    "Expression": {"SourceRef": {"Source": "n"}},
                                    "Property": "Name",
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
            resource_key, ds_id, model_id, report_id, "People With At Least One Dose"
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


class WVCountyVaccineRace(WVCountyVaccine):
    demographic = "race"
    demographic_query_name = "Race1"
    col_mapping = {
        "G0": "location_name",
        "M_1_DM3_0_C_1": "black",
        "M_1_DM3_1_C_1": "other",
        "M_1_DM3_2_C_1": "unknown",
        "M_1_DM3_3_C_1": "white",
    }
    variables = {
        "People With At Least One Dose": variables.INITIATING_VACCINATIONS_ALL,
        "People Fully Vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def construct_body(
        self, resource_key, ds_id, model_id, report_id, dose_type, counties
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
                                            # From
                                            ("d", "dimCounty", 0),
                                            ("c", "COVID_Vaccine", 0),
                                            ("n", "Navigation", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ("d", "County Name", "county"),
                                            (
                                                "c",
                                                f"{self.demographic_query_name}",
                                                "demographic",
                                            ),
                                        ],
                                        [
                                            # Aggregations
                                        ],
                                        [
                                            # Measures
                                            ("c", "Total Dose", "doses")
                                        ],
                                    ),
                                    "Where": [
                                        {
                                            "Condition": {
                                                "Not": {
                                                    "Expression": {
                                                        "In": {
                                                            "Expressions": [
                                                                {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "d"
                                                                            }
                                                                        },
                                                                        "Property": "County Name",
                                                                    }
                                                                }
                                                            ],
                                                            "Values": [
                                                                [
                                                                    {
                                                                        "Literal": {
                                                                            "Value": "null"
                                                                        }
                                                                    }
                                                                ]
                                                            ],
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
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "d"
                                                                    }
                                                                },
                                                                "Property": "County Name",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{counties[0]}'"
                                                                }
                                                            }
                                                        ],
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{counties[1]}'"
                                                                }
                                                            }
                                                        ],
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{counties[2]}'"
                                                                }
                                                            }
                                                        ],
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{counties[3]}'"
                                                                }
                                                            }
                                                        ],
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{counties[4]}'"
                                                                }
                                                            }
                                                        ],
                                                    ],
                                                }
                                            }
                                        },
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "n"
                                                                    }
                                                                },
                                                                "Property": "Name",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{dose_type}'"
                                                                }
                                                            }
                                                        ]
                                                    ],
                                                }
                                            }
                                        },
                                    ],
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
        counties = list(
            pd.read_csv(
                os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
            ).query("state == @self.state_fips and name != 'West Virginia'")["name"]
        )

        # Build post body
        jsons = {}
        for dose in ["People With At Least One Dose", "People Fully Vaccinated"]:
            data = []
            for i in range(0, len(counties), 5):
                print("sending: ", counties[i : i + 5])
                body = self.construct_body(
                    resource_key, ds_id, model_id, report_id, dose, counties[i : i + 5]
                )
                res = self.sess.post(url, json=body, headers=headers)
                data.append(res.json())
            jsons[dose] = data
        return jsons

    def normalize(self, resjson):
        # Extract components we care about from json

        data_rows = []
        for name, values in resjson.items():
            data = []
            for chunk in values:
                foo = chunk["results"][0]["result"]["data"]
                d = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
                data.extend(d)

            for record in data:
                flat_record = flatten_dict(record)

                row = {}
                for k in list(self.col_mapping.keys()):
                    flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                    if len(flat_record_key) > 0:
                        row[self.col_mapping[k]] = flat_record[flat_record_key[0]]
                        row["variable"] = name

                data_rows.append(row)

        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows).dropna()

        # Reshape
        out = df.melt(id_vars=["location_name", "variable"], var_name=self.demographic)
        # Add CMU, dt, vintage
        out = self.extract_CMU(out, self.variables, skip_columns=[self.demographic])
        out = out.assign(
            dt=self._retrieve_dt("US/Eastern"),
            vintage=self._retrieve_vintage(),
            value=lambda x: pd.to_numeric(x["value"].astype(str).str.replace(",", "")),
        )

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


class WVCountyVaccineAge(WVCountyVaccineRace):
    col_mapping = {
        "G0": "location_name",
        "M_1_DM3_0_C_1": "12-24",
        "M_1_DM3_1_C_1": "25-34",
        "M_1_DM3_2_C_1": "35-44",
        "M_1_DM3_3_C_1": "45-54",
        "M_1_DM3_4_C_1": "55-64",
        "M_1_DM3_5_C_1": "65-74",
        "M_1_DM3_6_C_1": "75-84",
        "M_1_DM3_7_C_1": "85_plus",
    }
    demographic_query_name = "Age Group"
    demographic = "age"


class WVCountyVaccineSex(WVCountyVaccineRace):
    demographic = "sex"
    demographic_query_name = "Sex"
    col_mapping = {
        "G0": "location_name",
        "M_1_DM3_0_C_1": "female",
        "M_1_DM3_1_C_1": "male",
        "M_1_DM3_2_C_1": "unknown",
    }
