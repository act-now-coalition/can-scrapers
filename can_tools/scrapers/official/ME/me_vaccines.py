from typing import Any

import pandas as pd
import os
import us

from can_tools.scrapers import CMU
from can_tools.scrapers import variables as v
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
                                                "c1",
                                                "COVID Vaccination Summary Measures",
                                                0,
                                            ),
                                            (
                                                "c",
                                                "COVID Vaccination Attributes",
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
                                            ("c", "Vaccine Manufacturer", "manu"),
                                        ],
                                        [],
                                        [
                                            (
                                                "c1",
                                                "Doses Administered",
                                                "total_vaccine_administered",
                                            ),
                                            (
                                                "c1",
                                                "First Dose",
                                                "total_vaccine_initiated",
                                            ),
                                            (
                                                "c1",
                                                "Final Dose",
                                                "total_vaccine_completed",
                                            ),
                                            (
                                                "c1",
                                                "Population First Dose %",
                                                "total_vaccine_initiated_percent",
                                            ),
                                            (
                                                "c1",
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
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
        data = [d for d in data if list(d.keys())[0] == "G0"]  # keep only relevent data

        # Build dict of dicts with relevant info
        col_mapping = {
            "G0": "county",
            "M_0_DM2_0_C_0": "total_vaccine_administered",
            "M_0_DM2_0_C_1": "pfizer_moderna_first_dose",
            "M_0_DM2_0_C_2": "total_vaccine_completed",
            "M_1_DM3_0_C_1": "janssen_series",
            "M_0_DM2_0_C_4": "total_vaccine_completed_percent",
        }

        # Iterate through all of the rows and store relevant data
        data_rows = []
        for record in data:
            flat_record = flatten_dict(record)
            row = {}
            for k in list(col_mapping.keys()):
                flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                if len(flat_record_key) > 0:
                    row[col_mapping[k]] = flat_record[flat_record_key[0]]

            data_rows.append(row)

        df = pd.DataFrame.from_records(data_rows)

        # calculate vaccine initiated to match def'n
        df["total_vaccine_initiated"] = (
            df["pfizer_moderna_first_dose"] + df["janssen_series"]
        )

        # Title case and remove the word county
        df["location_name"] = df["county"].str.replace("County, ME", "").str.strip()

        # Change % column into percentage
        df["total_vaccine_completed_percent"] = (
            100 * df["total_vaccine_completed_percent"]
        )

        # Reshape
        variables = {
            "total_vaccine_administered": v.TOTAL_DOSES_ADMINISTERED_ALL,
            "total_vaccine_initiated": v.INITIATING_VACCINATIONS_ALL,
            "total_vaccine_completed": v.FULLY_VACCINATED_ALL,
            "total_vaccine_completed_percent": v.PERCENTAGE_PEOPLE_COMPLETING_VACCINE,
        }

        out = self._reshape_variables(df, variables)
        out["dt"] = self._retrieve_dt("US/Eastern")
        return out


class MaineRaceVaccines(MicrosoftBIDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Maine").fips)

    source = "https://www.maine.gov/covid19/vaccines/dashboard"
    source_name = "Covid-19 Response Office of the Governor"
    powerbi_url = "https://wabi-us-east-a-primary-api.analysis.windows.net"

    demographic = "race"
    demographic_query_name = "Race"

    variables = {
        "initiated_total": v.INITIATING_VACCINATIONS_ALL,
        "complete_total": v.FULLY_VACCINATED_ALL,
    }

    col_mapping = {
        "G0": "county",
        "M_1_DM3_{demo}_M_0_DM4_0_C_0": "first_dose_total",
        "M_1_DM3_{demo}_M_0_DM4_0_C_1": "complete_total",
        "M_1_DM3_{demo}_M_1_DM5_0_C_2": "jj_complete",
        "M_1_DM3_{demo}_M_1_DM5_0_C_1": "jj_init",
    }

    # use these keys to fill in the keys for each dose type
    demographic_key = {
        "0": "ai_an",
        "1": "asian",
        "2": "black",
        "3": "pacific_islander",
        "4": "unknown",
        "5": "other",
        "6": "white",
    }  # unknown is 'not_provided'

    def construct_body(self, resource_key, ds_id, model_id, report_id, counties):
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
                                                "p",
                                                "Patient Census Demographic Attributes",
                                                0,
                                            ),
                                            (
                                                "c",
                                                "COVID Vaccination Summary Measures",
                                                0,
                                            ),
                                            ("c1", "COVID Vaccination Attributes", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            (
                                                "i",
                                                "Geographic County Name",
                                                "county",
                                            ),
                                            (
                                                "p",
                                                f"{self.demographic_query_name}",
                                                "demographic",
                                            ),
                                            (
                                                "c1",
                                                "Vaccine Manufacturer",
                                                "manufacturer",
                                            ),
                                        ],
                                        [],
                                        [
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
                                        ],
                                    ),
                                    "Where": [
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "i"
                                                                    }
                                                                },
                                                                "Property": "Geographic County Name",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{counties} County, ME'"
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
            ).query("state == @self.state_fips and name != 'Maine'")["name"]
        )

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
        # they report j&j doses in either the initiated or completed column--the other is empty (NA)
        df["jj_complete"] = df["jj_complete"].fillna(0)
        df["jj_init"] = df["jj_init"].fillna(0)

        # format, calculate total_vacccine_initiated + map CMU
        out = (
            df.dropna()
            .assign(
                initiated_total=lambda x: x["first_dose_total"]
                + x["jj_complete"]
                + x["jj_init"],
                vintage=self._retrieve_vintage(),
                dt=self._retrieve_dt("US/Eastern"),
                location_name=lambda x: x["county"].str.replace(" County, ME", ""),
            )
            .drop(columns={"county"})
            .pipe(
                self._reshape_variables,
                variable_map=self.variables,
                skip_columns=[self.demographic],
                id_vars=[self.demographic],
            )
        )
        return out


class MaineGenderVaccines(MaineRaceVaccines):
    demographic = "sex"
    demographic_query_name = "Gender"
    demographic_key = {
        "0": "female",
        "1": "male",
        "2": "unknown",
    }  # unknown is 'not provided' variabel in data


class MaineAgeVaccines(MaineRaceVaccines):
    demographic = "age"
    demographic_query_name = "Age Group"
    demographic_key = {
        "9": "0-11",
        "8": "12-15",
        "7": "16-19",
        "6": "20-29",
        "5": "30-39",
        "4": "40-49",
        "3": "50-59",
        "2": "60-69",
        "1": "70-79",
        "0": "80_plus",
    }

    def construct_body(self, resource_key, ds_id, model_id, report_id, counties):
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
                                                "p",
                                                "Patient Census Demographic Attributes",
                                                0,
                                            ),
                                            (
                                                "c",
                                                "COVID Vaccination Summary Measures",
                                                0,
                                            ),
                                            ("c1", "COVID Vaccination Attributes", 0),
                                        ]
                                    ),
                                    "OrderBy": [
                                        {
                                            "Direction": 2,
                                            "Expression": {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "p"}
                                                    },
                                                    "Property": "Age Group",
                                                }
                                            },
                                        }
                                    ],
                                    "Select": self.construct_select(
                                        [
                                            (
                                                "i",
                                                "Geographic County Name",
                                                "county",
                                            ),
                                            (
                                                "p",
                                                f"{self.demographic_query_name}",
                                                "demographic",
                                            ),
                                            (
                                                "c1",
                                                "Vaccine Manufacturer",
                                                "manufacturer",
                                            ),
                                        ],
                                        [],
                                        [
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
                                        ],
                                    ),
                                    "Where": [
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "i"
                                                                    }
                                                                },
                                                                "Property": "Geographic County Name",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{counties} County, ME'"
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
