from abc import ABC

import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import MicrosoftBIDashboard
from can_tools.scrapers.util import flatten_dict


class PennsylvaniaCountyVaccines(MicrosoftBIDashboard):
    """
    Fetch county level vaccine data from Pennsylvania's PowerBI dashboard
    """

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Pennsylvania").fips)

    source = "https://www.health.pa.gov/topics/disease/coronavirus/Vaccine/Pages/Dashboard.aspx"

    source_name = "Pennsylvania Department of Health"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

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
                                                "c",
                                                "Counts of People by County",
                                                0,
                                            )
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            ("c", "County", "location_name"),
                                            (
                                                "c",
                                                "PartiallyCovered",
                                                "total_vaccine_initiated",
                                            ),
                                            (
                                                "c",
                                                "FullyCovered",
                                                "total_vaccine_completed",
                                            ),
                                        ],
                                        [],
                                        [],
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

    def normalize(self, resjson):

        # Extract components we care about from json
        foo = resjson["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][0]["DM0"]

        # Build dict of dicts with relevant info
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}
        col_keys = list(col_mapping.keys())

        # Iterate through all of the rows and store relevant data
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
        df = df.query("location_name != '' & location_name != 'Out-of-State'")

        # Initiated is not at least one dose for PA -- it is a count of
        # individuals that are currently partially covered by a vaccine
        df["total_vaccine_initiated"] = df.eval(
            "total_vaccine_initiated + total_vaccine_completed"
        )

        # Make sure McKean follows capitalization in db
        df = df.replace({"location_name": {"Mckean": "McKean"}})

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
        out = df.melt(id_vars=["location_name"])

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


class PennsylvaniaVaccineDemographics(MicrosoftBIDashboard, ABC):
    """
    Fetch state level demographic vaccine data from Pennsylvania's PowerBI dashboard
    """

    has_location = False
    location_type = ""
    state_fips = int(us.states.lookup("Pennsylvania").fips)

    # source = "https://www.health.pa.gov/topics/disease/coronavirus/Vaccine/Pages/Vaccine.aspx"
    source = "https://www.health.pa.gov/topics/disease/coronavirus/Vaccine/Pages/Dashboard.aspx"

    source_name = "Pennsylvania Department of Health"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

    powerbi_table: str
    powerbi_dem_column: str
    demographic: str
    value_renamer: dict

    def _query(self):
        return {
            "Version": 2,
            "From": self.construct_from(
                [
                    (
                        "vg",
                        self.powerbi_table,
                        0,
                    ),
                    ("v", "Vaccinations_County_ID", 0),
                ]
            ),
            "Select": self.construct_select(
                [
                    ("v", "County", "county"),
                    ("vg", "Coverage", "coverage"),
                    ("vg", self.powerbi_dem_column, self.demographic),
                ],
                [("vg", "Total Count", 0, "count")],
                [],
            ),
        }

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
                                "Binding": {
                                    "DataReduction": {
                                        "DataVolume": 4,
                                        "Primary": {"Window": {"Count": 5000}},
                                    },
                                    "Primary": {
                                        "Groupings": [
                                            {"Projections": [0, 1, 2, 3]},
                                        ]
                                    },
                                },
                                "Query": self._query(),
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

    def clean_pa_location_names(self, df):
        return df.query(
            "location_name != '' & "
            "location_name != 'Out-Of-State' & "
            "location_name != 'Out-of-State'"
        ).replace({"location_name": {"Mckean": "McKean"}})

    def normalize_preprocess(self, resjson):
        # Extract components we care about from json
        foo = resjson["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]

        # Build dict of dicts with relevant info
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}
        col_keys = list(col_mapping.keys())
        value_dicts = foo["dsr"]["DS"][0]["ValueDicts"]

        # Iterate through all of the rows and store relevant data
        data_rows = []
        data_labels = [x["N"] for x in data[0]["S"]]
        row_dict = {x: 0 for x in data_labels}
        for record in data:
            # We zip the backwards lists because when a value doesn't change
            # (for example if same location) then it doesn't list that value
            # in the next row...
            row = dict(zip(data_labels[::-1], record["C"][::-1]))

            row_dict.update(row)

            data_rows.append(row_dict.copy())

        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows).dropna().rename(columns=col_mapping)

        return df, value_dicts

    def normalize_postprocess(self, df, untracked_cats, crename):
        out = self.extract_CMU(df, crename, columns=untracked_cats)
        out["dt"] = self._retrieve_dt("US/Eastern")
        out["vintage"] = self._retrieve_vintage()
        out.loc[:, "location_type"] = "county"
        out.loc[:, "location_type"] = out.loc[:, "location_type"].where(
            out.loc[:, "location_name"] != "Pennsylvania", "state"
        )
        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "location_type",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep].dropna()

    def normalize(self, resjson):
        df, vds = self.normalize_preprocess(resjson)

        # Replace indexes with values
        county_replacer = {i: vd for i, vd in enumerate(vds["D0"])}
        coverage_replacer = {i: vd for i, vd in enumerate(vds["D1"])}
        dem_replacer = {i: vd for i, vd in enumerate(vds["D2"])}
        df = (
            df.query("coverage < 2")
            .replace(
                {
                    "county": county_replacer,
                    "coverage": coverage_replacer,
                    self.demographic: dem_replacer,
                }
            )
            .rename(
                columns={
                    "county": "location_name",
                    "coverage": "variable",
                    "count": "value",
                }
            )
            .replace(
                {
                    "variable": {
                        "Partially Covered": "total_vaccine_initiated",
                        "Fully Covered": "total_vaccine_completed",
                    },
                    self.demographic: self.value_renamer,
                }
            )
            .pivot_table(
                index=["location_name", self.demographic],
                columns="variable",
                values="value",
            )
            .reset_index()
        )
        df = self.clean_pa_location_names(df)

        # Initiated is not at least one dose for PA
        df["total_vaccine_initiated"] = df.eval(
            "total_vaccine_initiated + total_vaccine_completed"
        )
        df = df.melt(id_vars=["location_name", self.demographic])

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

        categories = [
            "category",
            "measurement",
            "unit",
            "age",
            "sex",
            "race",
            "ethnicity",
        ]
        categories.remove(self.demographic)
        return self.normalize_postprocess(df, categories, crename)


class PennsylvaniaVaccineAge(PennsylvaniaVaccineDemographics):
    powerbi_table = "Vaccinations by Age_Group"
    powerbi_dem_column = "Age_Group"
    demographic = "age"
    value_renamer = {"105+": "105_plus"}


class PennsylvaniaVaccineEthnicity(PennsylvaniaVaccineDemographics):

    powerbi_table = "Vaccinations by Ethnicity"
    powerbi_dem_column = "Ethnicity"
    demographic = "ethnicity"
    value_renamer = {
        "Hispanic": "hispanic",
        "Not Hispanic": "non-hispanic",
        "Unknown": "unknown",
    }


class PennsylvaniaVaccineRace(PennsylvaniaVaccineDemographics):
    powerbi_table = "Vaccinations by Race"
    powerbi_dem_column = "Race"
    demographic = "race"
    value_renamer = {
        "White": "white",
        "African American": "black",
        "Asian": "asian",
        "Pacific Islander": "pacific_islander",
        "Native American": "ai_an",
        "Multiple/Other": "multiple_other",
        "Unknown": "unknown",
    }


class PennsylvaniaVaccineSex(PennsylvaniaVaccineDemographics):
    powerbi_table = "Vaccinations by Gender"
    powerbi_dem_column = "Gender"
    demographic = "sex"
    value_renamer = {"Male": "male", "Female": "female", "Unknown": "unknown"}
