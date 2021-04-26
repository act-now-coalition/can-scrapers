from typing import Any

import pandas as pd
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
    has_location = True
    location_type = "state"
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
                                                "p",
                                                "Patient Census Demographic Attributes",
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
                                                "p",
                                                "Race",
                                                "race",
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
        row_names[0] = "race"
        for record in data:
            data_rows.append(record["C"])

        # Dump records into a DataFrame
        race_info_replace = {
            "American Indian or Alaska Native": "ai_an",
            "Asian": "asian",
            "Black or African American": "black",
            "White": "white",
            "Native Hawaiian or Pacific Islander": "pacific_islander",
            "Other": "multiple_other",
            "Not Provided": "unknown",
        }
        df = pd.DataFrame.from_records(data_rows, columns=row_names)
        df["race"] = df["race"].map(race_info_replace)
        # Title case and remove the word county
        df["location"] = self.state_fips

        # # Change into percentage
        # for col in [
        #     "total_vaccine_initiated_percent",
        #     "total_vaccine_completed_percent",
        # ]:
        #     df.loc[:, col] = 100 * df.loc[:, col]

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
        out = df.melt(id_vars=["location", "race"], value_vars=crename.keys()).dropna()

        # Add CMU, dt, vintage
        out = self.extract_CMU(out, crename, ["category", "measurement", "unit", "age", "sex", "ethnicity"])
        out["dt"] = self._retrieve_dt("US/Eastern")
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
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

class MaineGenderVaccines(MicrosoftBIDashboard):
    has_location = True
    location_type = "state"
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
                                                "p",
                                                "Patient Census Demographic Attributes",
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
                                                "p",
                                                "Gender",
                                                "sex",
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
        row_names[0] = "sex"
        for record in data:
            data_rows.append(record["C"])

        gender_info_replace = {
            "Male": "male",
            "Female": "female",
            "Not Provided": "unknown"
        }
        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows, columns=row_names)
        df["sex"] = df["sex"].map(gender_info_replace)

        # Title case and remove the word county
        df["location"] = self.state_fips

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
        out = df.melt(id_vars=["location", "sex"], value_vars=crename.keys()).dropna()

        # Add CMU, dt, vintage
        out = self.extract_CMU(out, crename, ["category", "measurement", "unit", "age", "race", "ethnicity"])
        out["dt"] = self._retrieve_dt("US/Eastern")
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
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

class MaineAgeVaccines(MicrosoftBIDashboard):
    has_location = True
    location_type = "state"
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
                                                "p",
                                                "Patient Census Demographic Attributes",
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
                                                "p",
                                                "Age Group",
                                                "age",
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
            data_rows.append(record["C"])

        age_info_replace = {
            "Age 15 and Younger": "0-15",
            "Age 16-19": "16-19",
            "Age 20-29": "20-29",
            "Age 30-39": "30-39",
            "Age 40-49": "40-49",
            "Age 50-59": "50-59",
            "Age 60-69": "60-69",
            "Age 70-79": "70-79",
            "Age 80 and Older": "80_plus"
        }
        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows, columns=row_names)
        df["age"] = df["age"].map(age_info_replace)
        # Title case and remove the word county
        df["location"] = self.state_fips

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
        out = df.melt(id_vars=["location", "age"], value_vars=crename.keys()).dropna()

        # Add CMU, dt, vintage
        out = self.extract_CMU(out, crename, ["category", "measurement", "unit", "sex", "race", "ethnicity"])
        out["dt"] = self._retrieve_dt("US/Eastern")
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
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



