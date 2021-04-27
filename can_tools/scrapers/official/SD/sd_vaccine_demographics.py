import json
from typing import Any
import pandas as pd
import us
import os.path

from can_tools.scrapers import variables
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard


class SDVaccineSex(MicrosoftBIDashboard):

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("South Dakota").fips)

    source = "https://doh.sd.gov/COVID/Dashboard.aspx"
    source_name = "South Dakota Department of Health"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"
    demographic_query_name = "Sex"
    demographic = "sex"

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
    }

    def construct_body(self, resource_key, ds_id, model_id, report_id, counties):
        "Build body request"
        body = {}

        # Set version
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        from_variables = [
            # From
            ("c", "County", 0),
            ("v", "Vaccines", 0),
            ("m", " Measures", 0),
        ]

        where_query = [
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "c"}},
                                    "Property": "County",
                                }
                            }
                        ],
                        "Values": [
                            [{"Literal": {"Value": f"'{counties[0]}'"}}],
                            [{"Literal": {"Value": f"'{counties[1]}'"}}],
                            [{"Literal": {"Value": f"'{counties[2]}'"}}],
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
                                    "Expression": {"SourceRef": {"Source": "v"}},
                                    "Property": "IsMostRecentDose",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": "true"}}]],
                    }
                }
            },
        ]

        select_variables = [
            [
                # Selects
                ("c", "County", "county"),
                ("v", "Manufacturer - Dose # (spelled out)", "doses"),
                ("v", f"{self.demographic_query_name}", "demographic"),
            ],
            [],
            [
                # Measures
                ("m", "Number of Recipients", "recipients")
            ],
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
        # Get general information
        self._setup_sess()
        dashboard_frame = self.get_dashboard_iframe()
        resource_key = self.get_resource_key(dashboard_frame)
        ds_id, model_id, report_id = self.get_model_data(resource_key)

        # Get the post url
        url = self.powerbi_query_url()

        # Build post headers
        headers = self.construct_headers(resource_key)

        # get list of counties
        counties = list(
            pd.read_csv(
                os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
            ).query("state == @self.state_fips and name != 'South Dakota'")["name"]
        )

        jsons = []
        """
        --The max # of counties that the service will return at one time is 3 or 4 (depending on the size of the payload)--
        So, to get all counties make multiple requests.
        There are 66 counties, so we make 22 queries of 3 counties each.
        store the results of each in a list then return a list of lists.
        """
        for i in range(0, len(counties), 3):
            print("making request for: ", counties[i : i + 3])
            body = self.construct_body(
                resource_key, ds_id, model_id, report_id, counties[i : i + 3]
            )
            res = self.sess.post(url, json=body, headers=headers)
            jsons.append(res.json())
        return jsons

    def normalize(self, resjson):
        # get the demographic values and the order in which they appear from the fetch request (Extracts the list from the "ValueDicts" object)
        foo = resjson[0]["results"][0]["result"]["data"]
        demographic_values = foo["dsr"]["DS"][0]["ValueDicts"]["D1"]
        value_dicts = {k: v for k, v in enumerate(demographic_values)}

        # extract the data we want from each response chunk
        data = []
        for chunk in resjson:
            foo = chunk["results"][0]["result"]["data"]
            d = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
            data.extend(d)

        # extract data from fetch request into wide df
        # the manufacturer location/keys are hardcoded, but the demographic values use value_dicts
        dfs = []
        for key, value in value_dicts.items():
            col_mapping = {
                "G0": "county",
                f"M_1_DM3_0_M_1_DM5_{key}_C_1": f"jj",
                f"M_1_DM3_1_M_1_DM5_{key}_C_1": f"mod_1_dose",
                f"M_1_DM3_2_M_1_DM5_{key}_C_1": f"mod_complete",
                f"M_1_DM3_3_M_1_DM5_{key}_C_1": f"pf_1_dose",
                f"M_1_DM3_4_M_1_DM5_{key}_C_1": f"pf_complete",
            }
            data_rows = []
            for record in data:
                flat_record = flatten_dict(record)

                row = {}
                for k in list(col_mapping.keys()):
                    flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                    if len(flat_record_key) > 0:
                        row[col_mapping[k]] = flat_record[flat_record_key[0]]
                data_rows.append(row)

            # Dump records into a DataFrame and transform
            county = pd.DataFrame.from_records(data_rows)
            county[self.demographic] = value
            dfs.append(county.query("county == 'Codington'"))

        df = pd.concat(dfs)

        # calculate metrics to match our def'ns
        df["total_vaccine_initiated"] = df["jj"] + df["mod_1_dose"] + df["pf_1_dose"]
        df["total_vaccine_completed"] = (
            df["jj"] + df["mod_complete"] + df["pf_complete"]
        )

        out = self._rename_or_add_date_and_location(
            df,
            location_name_column="county",
            timezone="US/Central",
            location_names_to_drop=["Other"],
        )
        out = self._reshape_variables(
            out,
            self.variables,
            skip_columns=[self.demographic],
            id_vars=[self.demographic],
        ).dropna()
        out[self.demographic] = out[self.demographic].str.lower()

        return out.replace(
            {
                "Mccook": "McCook",
                "Mcpherson": "McPherson",
                "asian / pacific islander": "asian",
                "native american": "ai_an",
                "80+": "80_plus",
            }
        ).query("race != 'hispanic'")


class SDVaccineRace(SDVaccineSex):
    demographic_query_name = "Race Sanitized"
    demographic = "race"


class SDVaccineEthnicity(SDVaccineSex):
    demographic_query_name = "Ethnicity Sanitized"
    demographic = "ethnicity"


class SDVaccineAge(SDVaccineSex):
    demographic_query_name = "Age Range"
    demographic = "age"

    # age vals are stored in a different table, so a new query structure is needed
    def construct_body(self, resource_key, ds_id, model_id, report_id, counties):
        "Build body request"
        body = {}

        # Set version
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        from_variables = [
            # From
            ("c", "County", 0),
            ("a", "Age Ranges", 0),
            ("v", "Vaccines", 0),
            ("m", " Measures", 0),
        ]

        where_query = [
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "c"}},
                                    "Property": "County",
                                }
                            }
                        ],
                        "Values": [
                            [{"Literal": {"Value": f"'{counties[0]}'"}}],
                            [{"Literal": {"Value": f"'{counties[1]}'"}}],
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
                                    "Expression": {"SourceRef": {"Source": "v"}},
                                    "Property": "IsMostRecentDose",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": "true"}}]],
                    }
                }
            },
        ]

        select_variables = [
            [
                # Selects
                ("c", "County", "county"),
                ("v", "Manufacturer - Dose # (spelled out)", "doses"),
                ("a", f"{self.demographic_query_name}", "demographic"),
            ],
            [],
            [
                # Measures
                ("m", "Number of Recipients", "recipients")
            ],
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

    # since the age payload is larger, we can only fetch data for 2 counties at a time,
    # so a new fetch method is needed
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

        # get list of counties
        counties = list(
            pd.read_csv(
                os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
            ).query("state == @self.state_fips and name != 'South Dakota'")["name"]
        )

        jsons = []
        """
        --The max # of counties that the service will return at one time is 2--
        So, to get all counties make multiple requests.
        There are 66 counties, so we make 33 queries of 2 counties each
        store the results of each in a list then return a list of lists.
        """
        for i in range(0, len(counties), 2):
            print("making request for: ", counties[i : i + 2])
            body = self.construct_body(
                resource_key, ds_id, model_id, report_id, counties[i : i + 2]
            )
            res = self.sess.post(url, json=body, headers=headers)
            jsons.append(res.json())
        return jsons
