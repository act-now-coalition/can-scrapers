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
    demographic = "sex"
    select_variables = [
        [
            # Selects
            ("c", "County", "county"),
            ("v", "Manufacturer - Dose # (spelled out)", "doses"),
            ("v", "Sex", "demographic"),
        ],
        [],
        [
            # Measures
            ("m", "Number of Recipients", "recipients")
        ],
    ]
    from_variables = [
        # From
        ("c", "County", 0),
        ("v", "Vaccines", 0),
        ("m", " Measures", 0),
    ]

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
                            [{"Literal": {"Value": f"'{counties}'"}}],
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

        body["queries"] = [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": self.construct_from(self.from_variables),
                                    "Select": self.construct_select(
                                        *self.select_variables
                                    ),
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
        make one call per county to ensure that all the data is received
        """
        for county in counties:
            print("making request for: ", county)
            body = self.construct_body(resource_key, ds_id, model_id, report_id, county)
            res = self.sess.post(url, json=body, headers=headers)
            jsons.append(res.json())
        return jsons

    def normalize(self, resjson):
        # get the demographic and dose values and the order in which they appear from the fetch request (Extracts the list from the "ValueDicts" object)
        foo = resjson[0]["results"][0]["result"]["data"]
        demographic_values = foo["dsr"]["DS"][0]["ValueDicts"]["D1"]
        dose_values = foo["dsr"]["DS"][0]["ValueDicts"]["D0"]
        demo_value_dicts = {k: v for k, v in enumerate(demographic_values)}
        dose_value_dicts = {k: v for k, v in enumerate(dose_values)}

        # extract the data we want from each response chunk
        data = []
        for chunk in resjson:
            foo = chunk["results"][0]["result"]["data"]
            d = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
            data.extend(d)

        # loop through each county and parse all wanted data into list of dicts
        data_rows = []
        # for each county record
        for record in data:
            flat_record = flatten_dict(record)
            # get current dose
            for dose_key, dose_value in dose_value_dicts.items():
                row = {}
                row["county"] = flat_record["G0"]
                row["dose_type"] = dose_value
                # get current demo type
                for demo_key, demo_value in demo_value_dicts.items():
                    key = f"M_1_DM3_{dose_key}_M_1_DM5_{demo_key}_C_1"
                    if key in flat_record.keys():
                        row[demo_value] = flat_record.get(key)
                    # mark repeated values to be replaced/filled later
                    elif key not in flat_record.keys():
                        row[demo_value] = "REPEAT"
                data_rows.append(row)

        # manually forward fill the REPEAT values
        for i in range(0, len(data_rows)):
            record = data_rows[i]
            prev_record = data_rows[i - 1]
            keys = list(record.keys())
            max_demo_dict_key = max(demo_value_dicts.keys())

            for i in range(0, len(keys)):
                k = keys[i]
                prev_k = keys[i - 1]

                if record[k] == "REPEAT":
                    # if the entry is not the first in the list of demographics
                    # copy the value from the previous demographic
                    if k != demo_value_dicts[0]:
                        record[k] = record[prev_k]
                    # if the repeat is the first demograhic key, copy from last demographic value of previous record
                    else:
                        max_demo = demo_value_dicts[max_demo_dict_key]
                        record[k] = prev_record[max_demo]

        # dump into dataframe and pivot
        df = pd.DataFrame.from_records(data_rows)
        df = df.melt(id_vars=["county", "dose_type"], var_name=self.demographic)

        # calculate total_vaccine_initiated and total_vaccine_completed values
        init = (
            df.query(
                "dose_type in ['Pfizer - 1 dose', 'Moderna - 1 dose', 'Janssen - Series Complete']"
            )
            .groupby(["county", self.demographic])
            .sum()
            .reset_index()
            .assign(dose_type="total_vaccine_initiated")
        )
        complete = (
            df.query(
                "dose_type in ['Moderna - Series Complete','Pfizer - Series Complete', 'Janssen - Series Complete']"
            )
            .groupby(["county", self.demographic])
            .sum()
            .reset_index()
            .assign(dose_type="total_vaccine_completed")
        )

        out = self._rename_or_add_date_and_location(
            pd.concat([init, complete]),
            location_name_column="county",
            timezone="US/Central",
            location_names_to_drop=["Other"],
        )
        out = (
            self.extract_CMU(
                out,
                self.variables,
                skip_columns=[self.demographic],
                var_name="dose_type",
            )
            .assign(vintage=self._retrieve_vintage())
            .drop(columns="dose_type")
        )
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
    select_variables = [
        [
            # Selects
            ("c", "County", "county"),
            ("v", "Manufacturer - Dose # (spelled out)", "doses"),
            ("v", "Race Sanitized", "demographic"),
        ],
        [],
        [
            # Measures
            ("m", "Number of Recipients", "recipients")
        ],
    ]


class SDVaccineEthnicity(SDVaccineSex):
    demographic_query_name = "Ethnicity Sanitized"
    demographic = "ethnicity"
    select_variables = [
        [
            # Selects
            ("c", "County", "county"),
            ("v", "Manufacturer - Dose # (spelled out)", "doses"),
            ("v", "Ethnicity Sanitized", "demographic"),
        ],
        [],
        [
            # Measures
            ("m", "Number of Recipients", "recipients")
        ],
    ]


class SDVaccineAge(SDVaccineSex):
    demographic_query_name = "Age Range"
    demographic = "age"
    from_variables = [
        # From
        ("c", "County", 0),
        ("a", "Age Ranges", 0),
        ("v", "Vaccines", 0),
        ("m", " Measures", 0),
    ]
    select_variables = [
        [
            # Selects
            ("c", "County", "county"),
            ("v", "Manufacturer - Dose # (spelled out)", "doses"),
            ("a", "Age Range", "demographic"),
        ],
        [],
        [
            # Measures
            ("m", "Number of Recipients", "recipients")
        ],
    ]
