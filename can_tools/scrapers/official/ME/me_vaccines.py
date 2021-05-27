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
        return df

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

    demographic = 'race'
    demographic_query_name = 'Race'

    variables = {
        'initiated_total': v.INITIATING_VACCINATIONS_ALL,
        'complete_total': v.FULLY_VACCINATED_ALL,
    }

    col_mapping = {
        "G0":'county',
        "M_1_DM3_0_M_0_DM4_0_C_0": 'ai_an:first_dose_total',
        "M_1_DM3_0_M_0_DM4_0_C_1": 'ai_an:complete_total',
        "M_1_DM3_0_M_1_DM5_0_C_2": 'ai_an:jj_complete',

        "M_1_DM3_1_M_0_DM4_0_C_0": 'asian:first_dose_total',
        "M_1_DM3_1_M_0_DM4_0_C_1": 'asian:complete_total',
        "M_1_DM3_1_M_1_DM5_0_C_2": 'asian:jj_complete',

        "M_1_DM3_2_M_0_DM4_0_C_0": 'black:first_dose_total',
        "M_1_DM3_2_M_0_DM4_0_C_1": 'black:complete_total',
        "M_1_DM3_2_M_1_DM5_0_C_2": 'black:jj_complete',

        "M_1_DM3_3_M_0_DM4_0_C_0": 'nhpi:first_dose_total',
        "M_1_DM3_3_M_0_DM4_0_C_1": 'nhpi:complete_total',
        "M_1_DM3_3_M_1_DM5_0_C_2": 'nhpi:jj_complete',

        "M_1_DM3_4_M_0_DM4_0_C_0": 'not_provided:first_dose_total',
        "M_1_DM3_4_M_0_DM4_0_C_1": 'not_provided:complete_total',
        "M_1_DM3_4_M_1_DM5_0_C_2": 'not_provided:jj_complete',

        "M_1_DM3_5_M_0_DM4_0_C_0": 'other:first_dose_total',
        "M_1_DM3_5_M_0_DM4_0_C_1": 'other:complete_total',
        "M_1_DM3_5_M_1_DM5_0_C_2": 'other:jj_complete',

        "M_1_DM3_6_M_0_DM4_0_C_0": 'white:first_dose_total',
        "M_1_DM3_6_M_0_DM4_0_C_1": 'white:complete_total',
        "M_1_DM3_6_M_1_DM5_0_C_2": 'white:jj_complete',
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
                                                                        "Property": "Geographic County Name"
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
                                                            ]
                                                        }
                                                    }
                                                },
                                            ]
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
            if county == 'Sagadahoc':
                break
            print("making request for: ", county)
            body = self.construct_body(resource_key, ds_id, model_id, report_id, county)
            res = self.sess.post(url, json=body, headers=headers)
            jsons.append(res.json())
        return jsons

        return res.json()

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
            row = {}
            
            for k in list(self.col_mapping.keys()):
                flat_record_key = [frk for frk in flat_record.keys() if k in frk]
                if len(flat_record_key) > 0:
                    row[self.col_mapping[k]] = flat_record[flat_record_key[0]]
            data_rows.append(row)

        # combine into dataframe, melt, and separate demographics into new column
        df = pd.DataFrame.from_records(data_rows)
        df =  df.melt(id_vars=['county']).dropna()
        df[[self.demographic,'variable']] = df['variable'].str.split(':', expand=True)
        
        # sum jj_complete and first_dose rows to create total_vaccine_initiated
        df_init = df.query("variable in ['jj_complete', 'first_dose_total']")
        df_init = df_init.groupby(['county', self.demographic]).sum().assign(variable='initiated_total').reset_index()
        
        # re-combine
        df = pd.concat([df_init, df.query('variable == "complete_total"')])
        
        # format + map CMU
        out = (
            df.assign(
                vintage=self._retrieve_vintage(),
                dt=self._retrieve_dt('US/Eastern'),
                location_name=lambda x: x['county'].str.replace(' County, ME', '')
            )
            .drop(columns={'county'})
            .pipe(self.extract_CMU, cmu=self.variables, skip_columns=[self.demographic])
        )
        return out 

class MaineGenderVaccines(MaineRaceVaccines):
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Maine").fips)

    source = "https://www.maine.gov/covid19/vaccines/dashboard"
    source_name = "Covid-19 Response Office of the Governor"
    powerbi_url = "https://wabi-us-east-a-primary-api.analysis.windows.net"

    demographic = 'sex'
    demographic_query_name = 'Gender'

    col_mapping = {
        "G0":"county",
        "M_1_DM3_0_M_0_DM4_0_C_0":"female:first_dose_total",
        "M_1_DM3_0_M_0_DM4_0_C_1":"female:complete_total",
        "M_1_DM3_0_M_1_DM5_0_C_2":"female:jj_complete",
        "M_1_DM3_1_M_0_DM4_0_C_0":"male:first_dose_total",
        "M_1_DM3_1_M_0_DM4_0_C_1":"male:complete_total",
        "M_1_DM3_1_M_1_DM5_0_C_1":"male:jj_complete", # might change?
        "M_1_DM3_2_M_0_DM4_0_C_0":"not_provided:first_dose_total",
        "M_1_DM3_2_M_0_DM4_0_C_1":"not_provided:complete_total",
    }

class MaineAgeVaccines(MaineRaceVaccines):
    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Maine").fips)

    source = "https://www.maine.gov/covid19/vaccines/dashboard"
    source_name = "Covid-19 Response Office of the Governor"
    powerbi_url = "https://wabi-us-east-a-primary-api.analysis.windows.net"

    demographic = 'age'
    demographic_query_name = "Age Group"


    def normalize(self, resjson: dict) -> pd.DataFrame:
        # Extract components we care about from json
        foo = resjson[1]["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]

        # data = []
        # for chunk in resjson:
        #     foo = chunk["results"][0]["result"]["data"]
        #     d = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
        #     data.extend(d)

        row_names = [
            "age",
            "janssen_vaccine_completed",
            "moderna_vaccine_initiated",
            "moderna_vaccine_completed",
            "pfizer_vaccine_initiated",
            "pfizer_vaccine_completed",
            "total_vaccine_initiated",
            "total_vaccine_completed"
        ]

        # Iterate through all of the rows and store relevant data
        data_rows = []
        for record in data:
            if len(record["M"][1]["DM3"][0]["C"]) > 2:
                j_j_vacc = record["M"][1]["DM3"][0]["C"][2]
            else:
                j_j_vacc = record["M"][1]["DM3"][0]["C"][1]
            if(len(record["M"][1]["DM3"][1]["C"]) > 1):
                moderna1 = moderna2 = record["M"][1]["DM3"][1]["C"][1]
                if len(record["M"][1]["DM3"][1]["C"]) > 2:
                    moderna2 = record["M"][1]["DM3"][1]["C"][2]
                else:
                    moderna2 = 0
            else:
                moderna1 = 0
                moderna2 = 0
            if(len(record["M"][1]["DM3"]) > 2):
                if len(record["M"][1]["DM3"][2]["C"]) > 1:
                    pfizer1 = record["M"][1]["DM3"][2]["C"][1]
                    pfizer2 = 0
                    if len(record["M"][1]["DM3"][2]["C"]) > 2:
                        pfizer2 = record["M"][1]["DM3"][2]["C"][2]
                else:
                    pfizer1 = 0
                    pfizer2 = 0
            else:
                pfizer1 = 0
                pfizer2 = 0
            data_rows.append(
                (
                    record["G0"],
                    j_j_vacc,
                    moderna1,
                    moderna2,
                    pfizer1,
                    pfizer2,
                    j_j_vacc + record["M"][0]["DM2"][0]["C"][0],
                    record["M"][0]["DM2"][0]["C"][1]
                )
            )

        age_info_replace = {
            "Age 15 and Younger": "0-15",
            "Age 16-19": "16-19",
            "Age 20-29": "20-29",
            "Age 30-39": "30-39",
            "Age 40-49": "40-49",
            "Age 50-59": "50-59",
            "Age 60-69": "60-69",
            "Age 70-79": "70-79",
            "Age 80 and Older": "80_plus",
        }
        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows, columns=row_names)
        df["age"] = df["age"].map(age_info_replace)
        # Title case and remove the word county
        df["location"] = self.state_fips

        crename = {
            "janssen_vaccine_completed": CMU(
                category="janssen_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "moderna_vaccine_initiated": CMU(
                category="moderna_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "moderna_vaccine_completed": CMU(
                category="moderna_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "pfizer_vaccine_initiated": CMU(
                category="pfizer_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "pfizer_vaccine_completed": CMU(
                category="pfizer_vaccine_completed",
                measurement="cumulative",
                unit="people",
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
        }
        out = df.melt(id_vars=["location", "age"], value_vars=crename.keys()).dropna()

        # Add CMU, dt, vintage
        out = self.extract_CMU(
            out,
            crename,
            ["category", "measurement", "unit", "sex", "race", "ethnicity"],
        )
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
